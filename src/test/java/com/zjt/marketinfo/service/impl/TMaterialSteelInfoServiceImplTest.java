package com.zjt.marketinfo.service.impl;

import com.zjt.marketinfo.entities.TMaterialSteelInfo;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TMaterialSteelInfoServiceImplTest {

    @Autowired
    private TMaterialSteelInfoServiceImpl tMaterialSteelInfoService;
    @Test
    public void findUpALL() throws Exception {
        List<TMaterialSteelInfo> tMaterialSteelInfoList=tMaterialSteelInfoService.findUpALL();
        Assert.assertNotEquals(0,tMaterialSteelInfoList.size());
    }

}