package com.zjt.marketinfo.service.impl;

import com.zjt.marketinfo.entities.TMaterialSteelInfo;
import org.springframework.data.domain.Page;

import java.util.List;

public interface TMaterialSteelInfoService {
    //1.根据根据材料名称查询材料信息
    List<TMaterialSteelInfo> findOne()
    //2.查询所有钢材信息
    //Page<TMaterialSteelInfo>
}
