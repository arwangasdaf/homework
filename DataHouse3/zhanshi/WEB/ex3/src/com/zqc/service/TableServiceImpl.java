package com.zqc.service;

import com.zqc.bean.DominDay;
import com.zqc.dao.SearchDataImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author ZhuQichao
 * @create 2018/6/21 11:19
 **/
@Service("tableService")
public class TableServiceImpl implements TableService{
    @Override
    public List<DominDay> getTable(String startTime, String endTime, String domain, String behavior) {
        //返回 时间，behavior num 画这三者的图
        System.out.println("Tableservice........");
        String articleID = "";

        return SearchDataImpl.showAllData( startTime,  endTime,  domain,  behavior,  articleID);
    }
}
