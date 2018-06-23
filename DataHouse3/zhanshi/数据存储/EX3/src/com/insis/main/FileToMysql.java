package com.insis.main;

import com.insis.utils.PreFile;

import java.sql.SQLException;

/**
 * @author ZhuQichao
 * @create 2018/6/7 14:42
 **/
public class FileToMysql {

    public static void fileToMysql() throws SQLException{
//        PreFile.creatToMysql("./data/userEdu");
       // PreFile.creatToMysql("./data/articleInfo");
        //PreFile.creatToMysql("./data/userBasic");
        //PreFile.creatToMysql("./data/userInterest");
        PreFile.creatToMysql("./data/userSkill");
    }

    public static void main(String[] args) throws SQLException {
        fileToMysql();
    }

}
