package spark.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import scala.collection.JavaConverters;
import utils.GenresList;

import java.util.*;

public class SparkMLMain {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
                .setAppName("App")
                .setMaster("local[*]")
                .set("spark.jars", "mongo-spark-connector_2.12:3.0.0");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();


        // Load pipeline
        Pipeline pipeline = Pipeline.load("hdfs://hadoop-master:9000/user/root/model");

        JavaStreamingContext streamingContext = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), Durations.seconds(8));
        streamingContext.checkpoint("/root/spark-checkpoint");

        // Kafka consumer configuration
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", args[1]);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", args[2]);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList(args[0]);

        // Create Kafka direct stream
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        UserDefinedFunction getGenreNameById = functions.udf(
                (Integer genreId) -> GenresList.getGenreNameById(genreId),
                DataTypes.StringType
        );

        spark.udf().register("getGenre", getGenreNameById);


        // Process messages
        JavaDStream<String> data = messages.map(ConsumerRecord::value);

        data.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                List<String> jsonData = rdd.map(Object::toString).collect();
                if (!jsonData.isEmpty()) {

                    Dataset<Row> dataset = spark.read().json(spark.sparkContext().parallelize(JavaConverters.asScalaIteratorConverter(jsonData.iterator()).asScala().toSeq(), 1, scala.reflect.ClassTag$.MODULE$.apply(String.class)));
                    Dataset<Row> df = dataset.withColumn("primary_genre",
                                    functions.callUDF("getGenre", functions.element_at(functions.col("genre_ids"), 1).cast(DataTypes.IntegerType)))
                            .withColumn("release_year", functions.year(functions.to_date(functions.col("release_date"), "yyyy-MM-dd")))
                            .select("original_title", "primary_genre", "popularity", "vote_count", "vote_average", "release_year");

                    PipelineModel pipelineModel = pipeline.fit(df);
                    Dataset<Row> predictions = pipelineModel.transform(df);

                    Dataset<Row> result = df
                            .filter("original_title not like '%??%'")
                            .join(predictions, "original_title").select(df.col("*"), predictions.col("prediction").as("revenue"));

                    result.show();

                    result
                            .write()
                            .format("com.mongodb.spark.sql.DefaultSource")
                            .mode("append")
                            .option("spark.mongodb.output.uri", "localhost:27017/BigData.movie_revenue_prediction")
                            .save();
                }
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}