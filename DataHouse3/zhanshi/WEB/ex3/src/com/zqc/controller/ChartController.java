package com.zqc.controller;

import com.zqc.bean.DominDay;
import com.zqc.service.ChartService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author ZhuQichao
 * @create 2018/6/19 20:05
 **/

@Controller
public class ChartController {
    @Autowired
    ChartService chartService;

    @RequestMapping("chart.do")
    @ResponseBody

    public List<String> list(HttpSession session, HttpServletRequest request){
        System.out.println("controller.....");

//20150508,20150531,java,D0008129
        //20150512,20150531	Extjs	2	D0280895	5
        String startTime = request.getParameter("startTime");
        String endTime = request.getParameter("endTime");
        String domain0 = request.getParameter("domain");
        String articleID = request.getParameter("articleID");

        String time = null;
        String articleId = null;
        String domain = null;
        String behavior = null;
        String num = null;
        String percent = null;

        List<DominDay> list = chartService.getChart(startTime,endTime,domain0,articleID);
        List<String> threeList = new ArrayList<String>();
        DominDay dominDay = new DominDay();
        Iterator<DominDay> it = list.iterator();
        while (it.hasNext()) {
            dominDay = it.next();
            time = dominDay.getTime();
            articleId = dominDay.getArticleId();
            domain = dominDay.getDomain();
            behavior = dominDay.getBehavior();
            num = dominDay.getNum();
            if(num!=null) {
                percent = dominDay.getPercent();
            }

            System.out.println(time + "\t" + articleId + "\t" + domain + "\t" + behavior + "\t" + num + "\t" + percent);

            String line = time + "," + behavior + "," + num;
            threeList.add(line);

        }
        /*threeList.add("20150512,1,1");
        threeList.add("20150512,2,2");
        threeList.add("20150512,0,3");
        threeList.add("20150513,1,4");
        threeList.add("20150513,2,5");
        threeList.add("20150513,0,6");*/
        System.out.println("threeList: " +threeList.size());
       return threeList;
//20150526、20150528、python、behavior随意、D0000170
       //return new ArrayList<String>();
    }



}
