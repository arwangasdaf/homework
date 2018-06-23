package com.insis.main;

import com.insis.bean.UserBehavior;
import com.insis.utils.Config;
import com.insis.utils.PreFile;

import java.io.IOException;
import java.util.List;

/**
 * @author ZhuQichao
 * @create 2018/6/8 16:31
 **/
public class SortBehavior {

    public static void main(String[] args) {
        try {
            List<UserBehavior> list = PreFile.sortByTime(Config.filePath1);
            PreFile.sortedToWriteObject("./data/userBehavior1",list);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
