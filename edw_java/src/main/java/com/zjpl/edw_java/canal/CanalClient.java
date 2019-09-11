package com.zjpl.edw_java.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;

@Component
@ConfigurationProperties(prefix = "spring.canal")
public class CanalClient {
    protected final static Logger logger = LoggerFactory.getLogger(CanalClient.class);

    @Value("${spring.canal.destination}")
    private String destionation;

    @Value("${spring.canal.ip}")
    private String ip;

    @Value("${spring.canal.user}")
    private String username;
    @Value("${spring.canal.password}")
    private String password;

    protected Thread thread =null;
    protected volatile boolean running =false;
    @Autowired
    private CanalHander canalHander;

    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler(){

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error ",e);
        }
    };
    public void start(){
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip,11111),destionation,username,password);
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                canalHander.handler(running,connector);
            }
        });
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running=true;
    }
    public void stop(){
        if(!running){
            return;
        }
        running = false;
        if(thread !=null){
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        MDC.remove("destination");
    }
}
