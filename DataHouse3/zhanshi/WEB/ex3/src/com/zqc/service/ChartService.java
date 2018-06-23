package com.zqc.service;

import com.zqc.bean.DominDay;

import java.util.List;

/**
 * @author ZhuQichao
 * @create 2018/6/19 20:05
 **/
public interface ChartService {
    //����ʱ�䣬behavior num �������ߵ�ͼ
    public List<DominDay> getChart(String startTime, String endTime, String domain, String articleID);
}
