package io.confluent.scanner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;

/**
 * Flink scalar UDF — applies an approved data tag to a field in the
 * Confluent Stream Catalog.
 *
 * Mirrors the logic in review-api/catalog_client.py:
 *   1. GET {srUrl}/subjects/{subject}/versions/latest  → schema version
 *   2. POST {srUrl}/catalog/v1/entity/tags             → apply the tag
 *
 * SQL usage (Statement B — approval step):
 *   SELECT apply_tag(
 *       'https://psrc-xxx.aws.confluent.cloud',  -- SR_URL
 *       'SR_API_KEY',                             -- SR_API_KEY
 *       'SR_API_SECRET',                          -- SR_API_SECRET
 *       'lsrc-xxxxx',                             -- SR_CLUSTER_ID
 *       subject,
 *       field_path,
 *       tag
 *   ) AS result
 *   FROM (VALUES (...)) AS approvals(subject, field_path, tag);
 *
 * Returns "OK: {tag} → {subject}.{fieldPath}" on success, or "ERROR: ..." on failure.
 */
public class ApplyTagUDF extends ScalarFunction {

    private transient HttpClient httpClient;
    private transient ObjectMapper mapper;

    @Override
    public void open(FunctionContext context) {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
        this.mapper = new ObjectMapper();
    }

    /**
     * @param srUrl        Schema Registry base URL (also serves the Stream Catalog API)
     * @param srApiKey     Schema Registry API key
     * @param srApiSecret  Schema Registry API secret
     * @param srClusterId  Schema Registry cluster ID, e.g. "lsrc-xxxxx"
     * @param subject      Schema Registry subject, e.g. "payments-value"
     * @param fieldPath    Dot-notation field path, e.g. "customer.email"
     * @param tag          Tag to apply, e.g. "PII"
     * @return             "OK: ..." on success, "ERROR: ..." on failure
     */
    public String eval(
        String srUrl,
        String srApiKey,
        String srApiSecret,
        String srClusterId,
        String subject,
        String fieldPath,
        String tag
    ) {
        String base = srUrl.replaceAll("/+$", "");
        String authHeader = "Basic " + Base64.getEncoder().encodeToString(
            (srApiKey + ":" + srApiSecret).getBytes()
        );

        try {
            // ── Step 1: resolve latest schema version ──────────────────────
            int version = getLatestVersion(base, authHeader, subject);
            if (version < 0) {
                return "ERROR: could not resolve schema version for subject '" + subject + "'";
            }

            // ── Step 2: build qualified field name ─────────────────────────
            // Format: {srClusterId}:.:{subject}.v{version}.{fieldPath}
            String cleanPath = fieldPath.replace("[", ".").replace("]", "").replaceAll("\\.+", ".").replaceAll("^\\.", "");
            String qualifiedName = srClusterId + ":.:" + subject + ".v" + version + "." + cleanPath;

            // ── Step 3: apply the tag via Stream Catalog REST API ──────────
            String payload = mapper.writeValueAsString(new Object[]{
                new java.util.LinkedHashMap<String, Object>() {{
                    put("typeName", "sr_field");
                    put("attributes", new java.util.LinkedHashMap<String, String>() {{
                        put("qualifiedName", qualifiedName);
                    }});
                    put("classifications", new Object[]{
                        new java.util.LinkedHashMap<String, Object>() {{
                            put("typeName", tag);
                            put("attributes", new java.util.LinkedHashMap<String, String>() {{
                                put("classified_by", "flink-scanner-v1.0.0");
                            }});
                        }}
                    });
                }}
            });

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(base + "/catalog/v1/entity/tags"))
                .header("Content-Type", "application/json")
                .header("Authorization", authHeader)
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

            HttpResponse<String> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofString()
            );

            int status = response.statusCode();
            if (status == 200 || status == 201 || status == 204) {
                return "OK: " + tag + " → " + subject + "." + cleanPath;
            } else if (status == 409) {
                return "OK (already tagged): " + tag + " → " + subject + "." + cleanPath;
            } else {
                return "ERROR: HTTP " + status + " — " + response.body();
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "ERROR: interrupted";
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private int getLatestVersion(String base, String authHeader, String subject) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(base + "/subjects/" + subject + "/versions/latest"))
                .header("Authorization", authHeader)
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofString()
            );

            if (response.statusCode() == 200) {
                JsonNode node = mapper.readTree(response.body());
                return node.path("version").asInt(-1);
            }
        } catch (Exception e) {
            // fall through
        }
        return -1;
    }

    @Override
    public void close() {
        // nothing to clean up
    }
}
