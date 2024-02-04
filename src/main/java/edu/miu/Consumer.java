package edu.miu;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.miu.dto.Product;
import edu.miu.utils.CustomObjectMapper;
import edu.miu.utils.HbaseTable;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;

public class Consumer {

    public static void main(String[] args) throws Exception {
        ObjectMapper objectMapper = CustomObjectMapper.getMapper();
        SparkConf conf = new SparkConf().setAppName("first-topic-listener");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(5));

        Set<String> topics = new HashSet<>(Collections.singletonList("retail"));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

        HbaseTable.init();
        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        stream.foreachRDD(rdd -> {
            JavaRDD<Product> jrdd = rdd
                    .filter(f -> f._2 != null && !f._2.isEmpty())
                    .map(f -> objectMapper.readValue(f._2(), Product.class));
            jrdd.foreach(HbaseTable::populateData);
            return null;
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
