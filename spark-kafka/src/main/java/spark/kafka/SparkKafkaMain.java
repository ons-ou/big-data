package spark.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.apache.kafka.common.serialization.StringDeserializer;
import spark.stream.NameCounter;

import java.util.*;

public class SparkKafkaMain {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ALL);
        Logger.getLogger("akka").setLevel(Level.ALL);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.setAppName("App");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(8));
        streamingContext.checkpoint("/root/spark-checkpoint");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", args[1]);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", args[2]);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList(args[0]);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));
        JavaDStream<String> data = messages.map(ConsumerRecord::value);

        JavaPairDStream<String, ?> result = new NameCounter().process(data);

        result.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
