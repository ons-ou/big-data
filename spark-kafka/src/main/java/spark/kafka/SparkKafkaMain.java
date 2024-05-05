package spark.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;
import spark.stream.NameCounter;
import utils.GenresList;

import java.util.*;

public class SparkKafkaMain {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ALL);
        Logger.getLogger("akka").setLevel(Level.ALL);

        SparkConf conf = new SparkConf()
                .setAppName("App")
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.jars", "mongo-spark-connector_2.12:3.0.0");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        JavaStreamingContext streamingContext = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), Durations.seconds(8));
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

        JavaPairDStream<Tuple2<Integer, String>, Integer> result = new NameCounter().process(data);
        String collectionName = topics.contains("GENRES") ? "trending_genres" : "trending_keywords";

        result.foreachRDD(
                rdd -> {
                    // Convert each RDD to a DataFrame
                    JavaRDD<Row> rowRDD = rdd.map(tuple -> {
                        Tuple2<Integer, String> genre = tuple._1();
                        Integer id = genre._1();
                        String name = genre._2();
                        return RowFactory.create(id, name, tuple._2());
                    });
                    StructType schema = DataTypes.createStructType(new StructField[]{
                            DataTypes.createStructField("_id", DataTypes.IntegerType, false),
                            DataTypes.createStructField("name", DataTypes.StringType, true),
                            DataTypes.createStructField("count", DataTypes.IntegerType, true)
                    });
                    Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

                    df
                            .write()
                            .format("com.mongodb.spark.sql.DefaultSource")
                            .mode("append")
                            .option("spark.mongodb.output.uri", "localhost:27017/BigData." + collectionName)
                            .save();
                }
        );
        result.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
