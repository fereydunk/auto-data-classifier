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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
    // Per-instance cache of (subject + ":v" + version) → set of valid dotted
    // field paths from the live SR schema. SR versions are immutable, so no
    // TTL is needed — the cache is cleared whenever Flink reopens the UDF.
    private transient Map<String, Set<String>> fieldsCache;
    // Per-instance cache of (subject + ":v" + version) → [schemaId, namespace.recordName].
    private transient Map<String, String[]> metaCache;

    @Override
    public void open(FunctionContext context) {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
        this.mapper = new ObjectMapper();
        this.fieldsCache = new HashMap<>();
        this.metaCache = new HashMap<>();
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
            // Atlas format: {srClusterId}:.:{schemaId}:{namespace.recordName}.{fieldPath}
            String cleanPath = fieldPath.replace("[", ".").replace("]", "").replaceAll("\\.+", ".").replaceAll("^\\.", "");

            // ── Step 2b: SR-as-source-of-truth validation ──────────────────
            // Never POST a tag for a field that isn't in the live SR schema.
            // Fail closed if we can't fetch/parse the schema — Atlas would
            // otherwise return the cryptic "Type ENTITY with name null" error.
            String cacheKey = subject + ":v" + version;
            Set<String> validFields = fieldsCache.get(cacheKey);
            String[] meta = metaCache.get(cacheKey);  // {schemaId, namespace.recordName}
            if (validFields == null || meta == null) {
                Object[] result = getSchemaFieldPathsAndMeta(base, authHeader, subject, version);
                @SuppressWarnings("unchecked")
                Set<String> v = (Set<String>) result[0];
                String[] m = (String[]) result[1];
                validFields = v;
                meta = m;
                fieldsCache.put(cacheKey, validFields);
                metaCache.put(cacheKey, meta);
            }
            if (validFields.isEmpty() || meta[0] == null || meta[1] == null) {
                return "ERROR: could not fetch SR schema for subject '" + subject + "' v" + version;
            }
            if (!validFields.contains(cleanPath)) {
                return "ERROR: field '" + cleanPath + "' not in SR schema for '" + subject + "' v" + version;
            }

            String qualifiedName = srClusterId + ":.:" + meta[0] + ":" + meta[1] + "." + cleanPath;

            // ── Step 3: apply the tag via Stream Catalog REST API ──────────
            // Flat shape: [{entityType, entityName, typeName}].
            String payload = mapper.writeValueAsString(new Object[]{
                new java.util.LinkedHashMap<String, Object>() {{
                    put("entityType", "sr_field");
                    put("entityName", qualifiedName);
                    put("typeName", tag);
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

    /**
     * Fetch the registered Avro schema for (subject, version) and return the
     * full set of dotted field paths it contains. Returns an empty set on any
     * failure, which callers MUST treat as fail-closed.
     *
     * Mirrors review-api/sr_schema.py:extract_field_paths.
     */
    /**
     * Fetch SR schema, return {fieldPaths, [schemaId, "namespace.recordName"]}.
     * Empty set / null entries on any failure → caller must fail-closed.
     */
    private Object[] getSchemaFieldPathsAndMeta(
        String base, String authHeader, String subject, int version
    ) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(base + "/subjects/" + subject + "/versions/" + version))
                .header("Authorization", authHeader)
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofString()
            );
            if (response.statusCode() != 200) {
                return new Object[]{Collections.emptySet(), new String[]{null, null}};
            }

            JsonNode root = mapper.readTree(response.body());
            String schemaType = root.path("schemaType").asText("AVRO").toUpperCase();
            if (!"AVRO".equals(schemaType)) {
                return new Object[]{Collections.emptySet(), new String[]{null, null}};
            }
            String schemaText = root.path("schema").asText(null);
            if (schemaText == null || schemaText.isEmpty()) {
                return new Object[]{Collections.emptySet(), new String[]{null, null}};
            }
            JsonNode schemaNode = mapper.readTree(schemaText);
            Set<String> out = new HashSet<>();
            walkAvroFields(schemaNode, "", out);

            String sid = root.path("id").isInt() ? String.valueOf(root.path("id").asInt()) : null;
            String name = schemaNode.path("name").asText(null);
            String ns = schemaNode.path("namespace").asText(null);
            String recordQualifier = (name == null) ? null
                : (ns == null || ns.isEmpty()) ? name : ns + "." + name;
            return new Object[]{out, new String[]{sid, recordQualifier}};
        } catch (Exception e) {
            return new Object[]{Collections.emptySet(), new String[]{null, null}};
        }
    }

    /**
     * Recursively collect every dotted field path from an Avro schema node.
     * Handles record (with fields[]), union (e.g. ["null", record]), array
     * (with items), map (with values).
     */
    private void walkAvroFields(JsonNode node, String prefix, Set<String> out) {
        if (node == null || node.isNull()) return;
        if (node.isArray()) {
            for (JsonNode branch : node) {
                walkAvroFields(branch, prefix, out);
            }
            return;
        }
        if (!node.isObject()) return;

        JsonNode typeNode = node.get("type");
        String t = typeNode != null && typeNode.isTextual() ? typeNode.asText() : null;

        if ("record".equals(t)) {
            JsonNode fields = node.get("fields");
            if (fields != null && fields.isArray()) {
                Iterator<JsonNode> it = fields.elements();
                while (it.hasNext()) {
                    JsonNode f = it.next();
                    JsonNode nameNode = f.get("name");
                    if (nameNode == null || !nameNode.isTextual()) continue;
                    String childPath = prefix.isEmpty() ? nameNode.asText() : prefix + "." + nameNode.asText();
                    out.add(childPath);
                    walkAvroFields(f.get("type"), childPath, out);
                }
            }
        } else if ("array".equals(t)) {
            walkAvroFields(node.get("items"), prefix, out);
        } else if ("map".equals(t)) {
            walkAvroFields(node.get("values"), prefix, out);
        } else if (typeNode != null && (typeNode.isArray() || typeNode.isObject())) {
            // type is itself a union [..] or a nested complex type {..}
            walkAvroFields(typeNode, prefix, out);
        }
    }

    @Override
    public void close() {
        // nothing to clean up
    }
}
