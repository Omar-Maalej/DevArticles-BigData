package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class KafkaStreamProcessor {
    public static void main(String[] args) throws InterruptedException {
        // Configure Spark
        SparkConf conf = new SparkConf()
                .setAppName("KafkaStreamProcessor")
                .setMaster("local[*]");

        // Create streaming context with 5 second batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

        // Kafka consumer parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "kafka:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "spark-streaming-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Kafka producer configuration
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "kafka:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Topics to subscribe
        Collection<String> topics = Arrays.asList("articles");

        // Create direct Kafka stream
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        // Process the stream
        stream.foreachRDD(rdd -> {
            // Process each record to extract and count tags
            JavaPairRDD<String, Integer> tagCounts = rdd
                    .flatMap(record -> {
                        try {
                            JSONObject article = new JSONObject(record.value());
                            List<String> tags = new ArrayList<>();
                            
                            // Handle both tag_list (array) and tags (string) formats
                            if (article.has("tag_list")) {
                                JSONArray tagArray = article.getJSONArray("tag_list");
                                for (int i = 0; i < tagArray.length(); i++) {
                                    tags.add(tagArray.getString(i).toLowerCase().trim());
                                }
                            }
                            
                            if (article.has("tags")) {
                                String tagsStr = article.optString("tags", "");
                                if (!tagsStr.isEmpty()) {
                                    String[] tagArray = tagsStr.split("\\s*,\\s*");
                                    for (String tag : tagArray) {
                                        tags.add(tag.toLowerCase().trim());
                                    }
                                }
                            }
                            
                            return tags.iterator();
                        } catch (Exception e) {
                            System.err.println("Error parsing article: " + e.getMessage());
                            return Collections.emptyIterator();
                        }
                    })
                    .filter(tag -> !tag.isEmpty())
                    .mapToPair(tag -> new Tuple2<>(tag, 1))
                    .reduceByKey(Integer::sum);

            // Collect tag counts
            Map<String, Integer> tagCountMap = tagCounts.collectAsMap();
            
            // Create Kafka producer
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                // Send each tag count to Kafka
                for (Map.Entry<String, Integer> entry : tagCountMap.entrySet()) {
                    String message = String.format("{\"tag\":\"%s\",\"count\":%d}", 
                            entry.getKey(), entry.getValue());
                    producer.send(new ProducerRecord<>("tag_counts", entry.getKey(), message));
                }
                
                // Print summary
                System.out.println("\n=== Batch Summary ===");
                System.out.println("Processed " + rdd.count() + " articles");
                System.out.println("Found " + tagCountMap.size() + " unique tags");
                
                // Print top 10 tags
                System.out.println("\nTop 10 Tags:");
                tagCountMap.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(10)
                    .forEach(e -> System.out.printf("%-20s | %d%n", e.getKey(), e.getValue()));
            } catch (Exception e) {
                System.err.println("Error in Kafka producer: " + e.getMessage());
            }
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}