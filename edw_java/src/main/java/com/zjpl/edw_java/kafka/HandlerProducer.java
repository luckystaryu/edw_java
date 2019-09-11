package com.zjpl.edw_java.kafka;

import com.alibaba.fastjson.JSON;
import com.zjpl.edw_java.canal.CanalBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Component
public class HandlerProducer {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private KafkaProducerTask kafkaProducerTask;

    /**
     * 多线程同步提交
     * @param canalBean canal传递过来的bean
     * @param waiting  是否等待线程执行完成true:可以及时看到结果；false:让线程继续执行，并跳出此方法返回调用主程序；
     */
    public void sendMessage(CanalBean canalBean,boolean waiting){
        logger.info("111111");
        String canalBeanJsonStr = JSON.toJSONString(canalBean);
        Future<String> f =kafkaProducerTask.sendKafkaMessage(canalBean.getDatabase()+"."+canalBean.getTable(),canalBeanJsonStr);
          logger.info("HandlerProducer日志--->当前线程:" + Thread.currentThread().getName() + ",接受的canalBeanJsonStr:" + canalBeanJsonStr);
        if(waiting){
            try {
                f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
