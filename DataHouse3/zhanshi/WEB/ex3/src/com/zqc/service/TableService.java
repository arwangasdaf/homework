package com.zqc.service;

import com.zqc.bean.DominDay;

import java.util.List;

/**
 * @author ZhuQichao
 * @create 2018/6/21 11:18
 **/
public interface TableService {

    public List<DominDay> getTable(String startTime, String endTime, String domain, String behavior);
}
