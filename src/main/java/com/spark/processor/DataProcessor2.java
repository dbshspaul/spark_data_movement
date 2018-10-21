package com.spark.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
public class DataProcessor2 {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "I:\\hadoop-3.0.0");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("dataMovement")
                .getOrCreate();

        /*Dataset<Row> lines = */spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "mytopic")
                .load()
                .writeStream().format("json").option("path","g:/temp/stream/")
                .option("checkpointLocation","...")
                .start();

    }
}
