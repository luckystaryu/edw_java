//package com.zjpl.edw_java.spark.ad;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class AdClickRealTimeStateSpark {
//    public static void main(String[] args) throws InterruptedException {
//        SparkConf conf = new SparkConf()
//                .setAppName("AdClickRealTimeStateSpark")
//                .setMaster("local[*]");
//        //batch interval
//        JavaStreamingContext jssc= new JavaStreamingContext(conf, Durations.seconds(5));
//        //构建Kafka属性
//        Map<String,String> kafkaMap = new HashMap<String,String>();
//        //kafkaMap.put()
//        //2.创建kafka Dstream
//        JavaPairDStream<String,String>  javaPairDStream = KafkaUtils.createDirectStream();
//        jssc.start();
//        jssc.awaitTermination();
//        jssc.close();
//    }
//}
