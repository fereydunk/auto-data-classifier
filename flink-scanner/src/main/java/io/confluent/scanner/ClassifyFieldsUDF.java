package io.confluent.scanner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.Iterator;
import java.util.Map;

/**
 * Flink UDTF — Layer 1/2/3 field classifier.
 *
 * Called once per Kafka message. Parses the raw JSON payload, posts it
 * to the classifier service, and emits one output row per detected
 * (field_path, tag) pair — always keeping the highest-confidence entity
 * per pair across all three layers.
 *
 * Registered with: USING CONNECTIONS (`classifier-service`)
 * The connection provides network egress to the classifier endpoint.
 * The URL is still passed as a SQL parameter so it can be updated without
 * rebuilding the JAR.
 *
 * SQL usage:
 *   LATERAL TABLE(classify_fields(
 *       'https://classifier.example.com',  -- classifier service URL
 *       3,                                  -- max_layer: 1 | 2 | 3
 *       CAST(`$value` AS STRING)            -- raw JSON message
 *   ))
 *
 * Output columns: field_path, tag, score, layer, source, sample_value
 */
@FunctionHint(output = @DataTypeHint(
    "ROW<field_path STRING, tag STRING, score DOUBLE, layer INT, source STRING, sample_value STRING>"
))
public class ClassifyFieldsUDF extends TableFunction<Row> {

    // HttpClient is thread-safe and expensive to create — initialised once in open()
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
     * @param classifierUrl  Base URL of the classifier service, e.g. "https://classifier.example.com"
     * @param maxLayer       Classification depth: 1=field name only, 2=+regex, 3=+AI (default)
     * @param message        Raw JSON string from the Kafka message value
     */
    public void eval(String classifierUrl, Integer maxLayer, String message) {
        if (message == null || message.isBlank()) return;

        try {
            JsonNode messageNode = mapper.readTree(message);

            // Build the /classify request body
            ObjectNode requestBody = mapper.createObjectNode();
            requestBody.set("fields", messageNode);
            requestBody.put("max_layer", maxLayer != null ? maxLayer : 3);

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(classifierUrl.replaceAll("/+$", "") + "/classify"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(requestBody)))
                .build();

            HttpResponse<String> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofString()
            );

            if (response.statusCode() != 200) return;

            JsonNode responseNode = mapper.readTree(response.body());
            JsonNode detectedEntities = responseNode.get("detected_entities");
            if (detectedEntities == null || !detectedEntities.isObject()) return;

            // For each field path, emit the best (highest-score) entity per tag
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = detectedEntities.fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                String fieldPath = entry.getKey();
                JsonNode entities = entry.getValue();

                if (!entities.isArray()) continue;

                // Group by tag, keep highest-score entity per tag
                java.util.Map<String, JsonNode> bestPerTag = new java.util.LinkedHashMap<>();
                for (JsonNode entity : entities) {
                    String tag = entity.path("tag").asText("PII");
                    double score = entity.path("score").asDouble(0.0);
                    JsonNode current = bestPerTag.get(tag);
                    if (current == null || score > current.path("score").asDouble(0.0)) {
                        bestPerTag.put(tag, entity);
                    }
                }

                // Emit one row per (field_path, tag)
                for (Map.Entry<String, JsonNode> tagEntry : bestPerTag.entrySet()) {
                    JsonNode best = tagEntry.getValue();
                    String sampleValue = best.path("text_snippet").asText("");
                    if (sampleValue.length() > 50) {
                        sampleValue = sampleValue.substring(0, 47) + "...";
                    }

                    collect(Row.of(
                        fieldPath,
                        tagEntry.getKey(),
                        best.path("score").asDouble(0.0),
                        best.path("layer").asInt(3),
                        best.path("source").asText("ai_model"),
                        sampleValue
                    ));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Silently skip unparseable messages or transient HTTP failures
            // — the scanner is best-effort: missing one message is fine
        }
    }

    @Override
    public void close() {
        // HttpClient closes automatically; nothing to clean up
    }
}
