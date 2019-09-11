package com.zjt.marketinfo.service.impl;

import com.zjt.marketinfo.entities.TMaterialSteelInfo;
import com.zjt.marketinfo.service.TMaterialSteelInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TMaterialSteelInfoImpl implements TMaterialSteelInfoService {

    @Autowired
    private TMaterialSteelInfoService tMaterialSteelInfoService;
    @Override
    public List<TMaterialSteelInfo> findOne(String materialName) {
        List<TMaterialSteelInfo> tMaterialSteelInfoList = tMaterialSteelInfoService.findOne(materialName);
        return tMaterialSteelInfoList;
    }
}
