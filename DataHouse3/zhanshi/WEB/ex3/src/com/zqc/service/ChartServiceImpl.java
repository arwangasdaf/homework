package com.zqc.service;

import com.zqc.bean.DominDay;
import com.zqc.dao.SearchDataImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author ZhuQichao
 * @create 2018/6/19 20:07
 **/
@Service("chartService")
public class ChartServiceImpl implements ChartService{

    @Override
    public List<DominDay> getChart(String startTime, String endTime, String domain, String articleID) {
        //返回 时间，behavior num 画这三者的图
        System.out.println("service........");
		String behavior0 = "";
        //(String start0, String end0, String domain0, String behavior0, String articleId0)
        return SearchDataImpl.showAllData( startTime,  endTime,  domain,  behavior0,  articleID);
    }


}
