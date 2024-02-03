package edu.miu;

import kafka.serializer.StringDecoder;
import org.apache.commons.collections.IteratorUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class Consumer {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("first-topic-listener");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(5));

        Set<String> topics = new HashSet<>(Collections.singletonList("retail"));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(ssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaParams,
                topics);

        stream.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                // Recreate SparkContext within the closure
                SparkContext sc = new SparkContext(jsc.getConf());
                SQLContext sqlContext = new SQLContext(sc);

                // Define schema for DataFrame
                StructType schema = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("invoiceId", DataTypes.StringType, true),
                        DataTypes.createStructField("productId", DataTypes.StringType, true),
                        DataTypes.createStructField("productName", DataTypes.StringType, true),
                        DataTypes.createStructField("quantity", DataTypes.IntegerType, true),
                        DataTypes.createStructField("date", DataTypes.StringType, true),
                        DataTypes.createStructField("price", DataTypes.DoubleType, true),
                        DataTypes.createStructField("customerId", DataTypes.StringType, true),
                        DataTypes.createStructField("country", DataTypes.StringType, true)
                });


                List<Tuple2<String, String>> partitionList = IteratorUtils.toList(partition);
                JavaRDD<Tuple2<String, String>> partitionRDD = jsc.parallelize(partitionList);

                DataFrame df = sqlContext.createDataFrame(partitionRDD.map(tuple -> RowFactory.create(tuple._1(), tuple._2())), schema);

                // Register DataFrame as a temporary table
                df.registerTempTable("temp_table");

                // Insert DataFrame into Hive table
                sqlContext.sql("INSERT INTO TABLE retail_data SELECT * FROM temp_table");

                sc.stop(); // Stop the SparkContext when finished
            });
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
