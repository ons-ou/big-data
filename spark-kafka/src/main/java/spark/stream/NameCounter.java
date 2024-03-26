package spark.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NameCounter implements StreamProcessor<Integer> {
    @Override
    public JavaPairDStream<String, Integer> process(JavaDStream<String> messages) {
        JavaPairDStream<String, Integer> keywordCounts = messages.flatMapToPair(
                        record -> {
                            try {
                                ObjectMapper mapper = new ObjectMapper();
                                JsonNode[] jsonNodes = mapper.readValue(record, JsonNode[].class);

                                List<Tuple2<String, Integer>> results = new ArrayList<>();
                                for (JsonNode jsonNode : jsonNodes) {
                                    String name = jsonNode.get("name").asText();
                                    results.add(new Tuple2<>(name, 1));
                                }

                                return results.iterator();
                            } catch (IOException e) {
                                e.printStackTrace();
                                return Collections.emptyIterator();
                            }
                        });

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                (List<Integer> counts, Optional<Integer> previousState) -> {
                    int sum = previousState.orElse(0);
                    for (int count : counts) {
                        sum += count;
                    }
                    return Optional.of(sum);
                };

        return keywordCounts.updateStateByKey(updateFunction);

    }
}
