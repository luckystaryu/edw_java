package com.zjpl.edw_java.conf;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ConfigurationManagerTest {

    @Test
    public void getProperties() {
        String value=ConfigurationManager.getProperties("testkey");
        System.out.println("value==========>"+value);
    }
}
