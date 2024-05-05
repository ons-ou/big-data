package hadoop.mapreduce;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MapReduceMain {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        OperationTypes type = OperationTypes.valueOf(args[0]);

        MapReduceJob jobCreator = null;
        switch (type){
            case DIST_BY_GENRE:
                jobCreator = new DistributionByGenre();
                break;
            case AVG_RATE_BY_GENRE:
                jobCreator = new AvgReviewByGenre();
                break;
            case AVG_RATE_BY_COMPANY:
                jobCreator = new AvgReview();
                break;
            case KEYWORD_COUNT:
                jobCreator = new KeywordCount();
        }

        Job job = jobCreator.createJob();

        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path outputPath = new Path(args[2]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        if (type == OperationTypes.DIST_BY_GENRE) {
            MultipleOutputs.addNamedOutput(job, "combiner", TextOutputFormat.class, Text.class, IntWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);
        }

        if(job.waitForCompletion(true)){
            processOutputFiles(outputPath, type.getValue());
            if ( type == OperationTypes.DIST_BY_GENRE){
                processOutputFiles(new Path(args[2], "by_year"), "dist_by_genre_by_year");
            }
            System.exit(0);
        } else {
            System.exit(1);
        }
        System.exit(1);

    }

    private static void processOutputFiles(Path outputPath, String collectionName) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());

        FileStatus[] status = fs.listStatus(outputPath);
        List<String> update = new ArrayList<>();
        for (FileStatus fileStatus : status) {
            if (!fileStatus.isDirectory()) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())))) {
                String line;
                List<Document> docs = new ArrayList<>();
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\t");
                    String field = fields[1];

                    Number number;
                    if (field.contains(".")) {
                        number = Float.parseFloat(field);
                    } else {
                        number = Integer.parseInt(field);
                    }
                    Document doc;
                    if (!Objects.equals(collectionName, "dist_by_genre_by_year")) {
                        doc = new Document("name", fields[0])
                                .append("count", number);
                        update.add(doc.get("name").toString());

                    } else {
                        doc = new Document("genre", fields[0].split(",")[0])
                                .append("year",  Integer.parseInt(fields[0].split(",")[1]))
                                .append("count", number);
                        update.add(doc.get("genre").toString());

                    }
                    docs.add(doc);
                }

                if (!docs.isEmpty()) {
                    String uri = "localhost:27017";
                    ConnectionString mongoURI = new ConnectionString(uri);
                    MongoClientSettings settings = MongoClientSettings.builder()
                            .applyConnectionString(mongoURI)
                            .build();
                    MongoClient mongoClient = MongoClients.create(settings);
                    MongoDatabase database = mongoClient.getDatabase("BigData");
                    MongoCollection<Document> collection = database.getCollection(collectionName);
                    if (!update.isEmpty()) {
                        collection.deleteMany(Filters.in("name", update));
                    }
                    collection.insertMany(docs);
                    mongoClient.close();
                }
            }}
        }
    }
}
