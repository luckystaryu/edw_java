package com.zjpl.edw_java.kafka;

import com.zjpl.edw_java.canal.BaseCanalClientTest;

public class AbstractKafkaTest extends BaseCanalClientTest {
    public static String topic ="example";
    public static Integer partition = null;
    public static String groupId = "g4";
    public static String servers ="172.16.1.61:9092,172.16.1.62:9092,172.16.1.63:9092";
    public static String zkServers ="172.16.1.61:2181,172.16.1.62:2181,172.16.1.63:2181";
    public void sleep(long time){
        try {
            thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
