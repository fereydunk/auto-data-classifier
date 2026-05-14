package io.confluent.scanner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

/**
 * Flink UDTF — Schema Registry version watcher.
 *
 * Called for each incoming message on the source topic (the source topic acts
 * as a heartbeat). Rate-limited internally so it only checks Schema Registry
 * at most once per CHECK_INTERVAL_MS regardless of message volume.
 *
 * Emits a single row whenever the schema version for the watched subject
 * increases — i.e., whenever a schema evolution event occurs.  The emitted
 * row is written to the scan-triggers topic by the schema-evolution statement,
 * which causes the unified scan driver to immediately classify recent messages.
 *
 * SQL usage (schema-evolution trigger producer):
 *   INSERT INTO `{topic}-scan-triggers`
 *   SELECT 'schema_evolution' AS trigger_type,
 *          '{topic}'          AS source_topic,
 *          triggered_at
 *   FROM `{topic}`,
 *        LATERAL TABLE(
 *            schema_watcher('{sr_url}', '{sr_key}', '{sr_secret}', '{topic}-value')
 *        );
 *
 * Output columns: triggered_at TIMESTAMP(3)
 */
@FunctionHint(output = @DataTypeHint("ROW<triggered_at TIMESTAMP(3)>"))
public class SchemaWatcherUDF extends TableFunction<Row> {

    // How often to actually hit Schema Registry, regardless of message rate.
    // Set to 60 s so a busy topic doesn't hammer the SR API.
    private static final long CHECK_INTERVAL_MS = 60_000L;

    private transient HttpClient httpClient;
    private transient ObjectMapper mapper;
    private transient String authHeader;

    // Persisted for the lifetime of the Flink job (instance variable, not serialised state).
    // If the job restarts, lastKnownVersion resets to -1 and the first check
    // initialises it without emitting a spurious trigger.
    private transient int  lastKnownVersion = -1;
    private transient long lastCheckMs      = 0L;

    @Override
    public void open(FunctionContext ctx) {
        httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
        mapper = new ObjectMapper();
        // authHeader is built lazily on first eval() call when credentials are available
    }

    /**
     * @param srUrl    Schema Registry base URL
     * @param apiKey   Schema Registry API key
     * @param apiSecret Schema Registry API secret
     * @param subject  Subject to watch, e.g. "payments-value"
     */
    public void eval(String srUrl, String apiKey, String apiSecret, String subject) {
        long nowMs = System.currentTimeMillis();

        // Rate-limit: skip if we checked recently
        if (nowMs - lastCheckMs < CHECK_INTERVAL_MS) return;
        lastCheckMs = nowMs;

        if (authHeader == null) {
            authHeader = "Basic " + Base64.getEncoder().encodeToString(
                (apiKey + ":" + apiSecret).getBytes()
            );
        }

        int currentVersion = fetchLatestVersion(srUrl.replaceAll("/+$", ""), subject);
        if (currentVersion < 0) return; // SR unreachable — skip silently

        if (lastKnownVersion < 0) {
            // First successful check: record version, do NOT emit (no baseline to compare)
            lastKnownVersion = currentVersion;
            return;
        }

        if (currentVersion > lastKnownVersion) {
            // Schema evolved — emit a trigger row
            lastKnownVersion = currentVersion;
            collect(Row.of(
                java.sql.Timestamp.from(Instant.ofEpochMilli(nowMs))
            ));
        }
    }

    private int fetchLatestVersion(String base, String subject) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(base + "/subjects/" + subject + "/versions/latest"))
                .header("Authorization", authHeader)
                .header("Accept", "application/json")
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
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
            // Network hiccup — will retry on next check interval
        }
        return -1;
    }

    @Override
    public void close() {
        // HttpClient closes automatically
    }
}
