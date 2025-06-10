package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.*;

public class KafkaStreamProcessor {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("KafkaStreamProcessor").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "spark-streaming-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Collection<String> topics = Arrays.asList("articles");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        stream.foreachRDD(rdd -> {
            // TAG COUNTS
            JavaPairRDD<String, Integer> tagCounts = rdd.flatMap(record -> {
                try {
                    JSONObject article = new JSONObject(record.value());
                    List<String> tags = new ArrayList<>();

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
                    System.err.println("Error parsing tags: " + e.getMessage());
                    return Collections.emptyIterator();
                }
            })
            .filter(tag -> !tag.isEmpty())
            .mapToPair(tag -> new Tuple2<>(tag, 1))
            .reduceByKey(Integer::sum);

            // LANGUAGE DISTRIBUTION
            JavaPairRDD<String, Integer> languageCounts = rdd.flatMap(record -> {
                try {
                    JSONObject article = new JSONObject(record.value());
                    String lang = article.get("language").toString().toLowerCase().trim();
                    return Collections.singletonList(lang).iterator();
                } catch (Exception e) {
                    System.err.println("Error extracting language: " + e.getMessage());
                    return Collections.emptyIterator();
                }
            })
            .filter(lang -> !lang.isEmpty())
            .mapToPair(lang -> new Tuple2<>(lang, 1))
            .reduceByKey(Integer::sum);

            // SEND TO KAFKA
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                // Send tags
                Map<String, Integer> tagMap = tagCounts.collectAsMap();
                tagMap.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(10)
                    .forEach(entry -> {
                        String json = String.format("{\"tag\":\"%s\",\"count\":%d}", entry.getKey(), entry.getValue());
                        producer.send(new ProducerRecord<>("tag_counts", entry.getKey(), json));
                    });

                // Send language distribution
                Map<String, Integer> langMap = languageCounts.collectAsMap();
                langMap.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .forEach(entry -> {
                        String json = String.format("{\"language\":\"%s\",\"count\":%d}", entry.getKey(), entry.getValue());
                        producer.send(new ProducerRecord<>("language_distribution", entry.getKey(), json));
                    });

                // Logging
                System.out.println("\nProcessed " + rdd.count() + " articles");
                System.out.println("Top Tags: " + tagMap);
                System.out.println("Language Distribution: " + langMap);
            } catch (Exception e) {
                System.err.println("Error sending to Kafka: " + e.getMessage());
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
