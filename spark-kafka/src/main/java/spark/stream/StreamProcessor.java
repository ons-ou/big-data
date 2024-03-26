package spark.stream;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;

public interface StreamProcessor<V> {

    JavaPairDStream<String, V> process(JavaDStream<String> messages);

}
