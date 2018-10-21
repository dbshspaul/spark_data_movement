package com.spark.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/*
*created by Debashis Paul on 13-Oct-18 8:10 PM
*/
public class DataProcessor1 {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "I:\\hadoop-3.0.0");


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers","localhost:9092,192.168.1.101:9092");
        kafkaParams.put("key.deserializer",StringDeserializer.class);
        kafkaParams.put("value.deserializer",ByteArrayDeserializer.class);
        kafkaParams.put("group.id","group_1");
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("enable.auto.commit",false);

        Collection<String> topics = Arrays.asList("mytopic");

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("dataMovement");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(30));

        final JavaInputDStream<ConsumerRecord<String, byte[]>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, byte[]>Subscribe(topics, kafkaParams)
                );

        JavaPairDStream<String, byte[]> stringStringJavaPairDStream = stream.mapToPair(
                (PairFunction<ConsumerRecord<String, byte[]>, String, byte[]>) record -> new Tuple2<>(record.key(), record.value()));

        JavaDStream<String> stringLine = stringStringJavaPairDStream.map(tuple -> new String(tuple._2));

        stringLine.foreachRDD(rdd ->{
            if(!rdd.isEmpty()){
                rdd.coalesce(1).saveAsTextFile("g:\\temp\\count\\");
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
