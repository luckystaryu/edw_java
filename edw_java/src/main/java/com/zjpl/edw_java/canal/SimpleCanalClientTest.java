package com.zjpl.edw_java.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.net.InetSocketAddress;
import java.util.Properties;


public class SimpleCanalClientTest extends AbstarctCanalClientTest {
    public SimpleCanalClientTest(String destination) {
        super(destination);
    }

    public static void main(String[] args) {
        //根据ip,直接创建连接，无HA的功能
        String destination = "example";
        String ip = "172.16.1.61";
        String topic = "heyang";
        String kafka = "172.16.1.61:9092,172.16.1.62:9092,172.16.1.63:9092";
        String table = "policy_cred";
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111),
                destination,
                "canal",
                "canal");
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka);
        props.put("request.required.acks", 1);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);//32m
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        final SimpleCanalClientTest clientTest = new SimpleCanalClientTest(destination);
        clientTest.setConnector(connector);
        clientTest.setKafkaProducer(producer);
        clientTest.setTopic(topic);
        clientTest.setTable(table);
        clientTest.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    logger.info("## stop the canal client");
                    clientTest.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:", e);
                } finally {
                    logger.info("##canal cleint is down.");
                }
            }
        });
    }
}
