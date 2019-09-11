package com.zjpl.edw_java.kafka;

import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaClientRunningTest extends AbstractKafkaTest {
    private Logger logger = LoggerFactory.getLogger(KafkaClientRunningTest.class);
    private boolean running = true;
    public void testKafkaCoumser(){
        final ExecutorService excutor = Executors.newFixedThreadPool(1);
        final KafkaCanalConnector connector = new KafkaCanalConnector(servers,topic,partition,groupId,null,false);
        excutor.submit(new Runnable() {
            @Override
            public void run() {
                connector.connect();
                connector.subscribe();
                while (running) {
                    List<Message> messages = connector.getList(3L, TimeUnit.SECONDS);
                    if (messages != null) {
                        System.out.println(messages);
                    }
                    connector.ack();
                }
                connector.unsubscribe();
                connector.disconnect();
            }
        });
        sleep(60000);
        running =false;
        excutor.shutdown();
        logger.info("shutdown completed");
    }
}
