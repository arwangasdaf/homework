package com.zqc.controller;

import com.zqc.bean.DominDay;
import com.zqc.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.persistence.Table;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author ZhuQichao
 * @create 2018/6/21 11:17
 **/
@Controller
public class TableController {
    @Autowired
    TableService tableService;

    @RequestMapping("table.do")
    @ResponseBody

    public List<String> list(HttpSession session, HttpServletRequest request){
        String startTime = request.getParameter("startTime");
        String endTime = request.getParameter("endTime");
        String domain = request.getParameter("domain");
        String behavior = request.getParameter("behavior");

        String time0 = null;
        String articleId0 = null;
        String domain0 = null;
        String behavior0 = null;
        String num = null;
        String percent = null;

        List<DominDay> list = tableService.getTable(startTime,endTime,domain,behavior);
        List<String> threeList = new ArrayList<String>();
        DominDay dominDay = new DominDay();
        Iterator<DominDay> it = list.iterator();
        while (it.hasNext()) {
            dominDay = it.next();
            time0 = dominDay.getTime();
            articleId0= dominDay.getArticleId();
            domain0 = dominDay.getDomain();
            behavior0 = dominDay.getBehavior();
            num = dominDay.getNum();
            if(num!=null) {
                percent = dominDay.getPercent();
            }

            String line = time0 + "," +domain0 + "," + articleId0 + "," + behavior0 + "," + num + "," + percent;
            System.out.println(line);
            threeList.add(line);

        }

        System.out.println("threeListTable: " +threeList.size());
        return threeList;

    }
}
