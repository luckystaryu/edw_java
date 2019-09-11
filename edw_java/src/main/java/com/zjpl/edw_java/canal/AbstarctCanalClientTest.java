package com.zjpl.edw_java.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.zjpl.edw_java.kafka.kafkaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.MDC;
import org.springframework.util.Assert;


@Slf4j
public abstract class AbstarctCanalClientTest extends BaseCanalClientTest {

    public AbstarctCanalClientTest(String destination) {
        this(destination,null);
    }

    public AbstarctCanalClientTest(String destination, CanalConnector connector) {
        this.destination = destination;
        this.connector = connector;
    }

    public AbstarctCanalClientTest(String destination, CanalConnector connector, Producer<String,String> kafkaProducer) {
        this.connector = connector;
        this.destination = destination;
        this.kafkaProducer = kafkaProducer;
    }

    /**
     * 启动canal client
     */
    protected void start(){
        Assert.notNull(connector,"connector is null");
        Assert.notNull(kafkaProducer,"Kafka producer configuration is null");
        Assert.notNull(topic,"kafka topic is null");
        thread = new Thread(new Runnable(){
            public void run(){
                process();
            }
        });
        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    /**
     * 停止canal client
     */
    protected void stop(){
        if(!running){
            return;
        }
        running = false;
        if(thread!=null){
            try{
                thread.join();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        kafkaProducer.close();
        MDC.remove("destination");
    }

    /**
     * 主流程：处理从canal server获取的数据
     */
    protected void process(){
        int batchSize = 5*1024;
        while(running){
            try{
                MDC.put("destination",destination);
                //建立连接，订阅表
                connector.connect();
                connector.subscribe("materialfac\\..*");
                while(running){
                    Message message = connector.getWithoutAck(batchSize);//获取指定数量的数据
                    long batchId = message.getId();
                    try{
                        int size = message.getEntries().size();
                        if(batchId == -1||size ==0){
                            logger.warn("no message....");
                            thread.sleep(2000);
                        }else{
                            //简单处理，打印数据
                            //printSummary(message,batchId,size);
                            //printEntry(message.getEntries());
                            //kafkaService.sendMessage(message.getEntries());
                            kafkaEntry(message.getEntries());
                        }
                        connector.ack(batchId);//提交确认
                    }catch (Exception e){
                        //处理失败回滚数据
                        e.printStackTrace();
                        connector.rollback();
                    }
                }
            }catch (Exception e){
                logger.error("process error!",e);
            }finally {
                //断开连接
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

}
