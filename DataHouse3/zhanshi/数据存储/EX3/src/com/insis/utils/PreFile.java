package com.insis.utils;

import com.insis.bean.UserBehavior;
import com.insis.dao.MysqlDao;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author ZhuQichao
 * @create 2018/6/7 11:59
 **/
public class PreFile {
    public PreFile() {

    }

    public static FileInputStream fis = null;
    public static InputStreamReader isr = null;
    public static BufferedReader br = null;
    public static OutputStream out = null;


    public static void creatToMysql(String inpath) throws SQLException {
        int lenName = inpath.split("/").length;
        String tableName = inpath.split("/")[lenName - 1];
        MysqlDao.creatTable(tableName);
        try {
            read(inpath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void read(String inpath) throws IOException {
        File file = new File(inpath);
        BufferedReader br = new BufferedReader(new FileReader(file));
        int lenName = inpath.split("/").length;
        String tableName = inpath.split("/")[lenName - 1];
        int fileLen = FileLengh.valueOf(tableName).getValue();
        String line = null;
        Connection conn = ConnectMysql.getConnection();
        while ((line = br.readLine()) != null) {
            String insertLine = fillNaN(line, fileLen);

            MysqlDao.insertPerData(tableName, insertLine, conn);
        }
        ConnectMysql.closeConnection();


    }

    public static List<UserBehavior> sortByTime(String inpath) throws IOException {
        File file = new File(inpath);//对文件按日期排序
        BufferedReader br = new BufferedReader(new FileReader(file));
        int lenName = inpath.split("/").length;
        String tableName = inpath.split("/")[lenName - 1];
        int fileLen = FileLengh.valueOf(tableName).getValue();
        String line = null;
        List<UserBehavior> list = new ArrayList<UserBehavior>();
        while ((line = br.readLine()) != null) {
            if (line.split("\001").length == fileLen) {
                list.add(new UserBehavior(line));
                System.out.println("list add......");
            }
        }
        Collections.sort(list);
        System.out.println("sort sucess!");
        return list;
    }
    public static List<String> replaceSpace(String inpath) throws IOException {
        File file = new File(inpath);//对文件按日期排序
        BufferedReader br = new BufferedReader(new FileReader(file));
        int lenName = inpath.split("/").length;
        String tableName = inpath.split("/")[lenName - 1];
        int fileLen = FileLengh.valueOf(tableName).getValue();
        String line = null;
        List<String> list = new ArrayList<String>();
        while ((line = br.readLine()) != null) {
            if (line.split("\001").length == fileLen) {
                list.add(line.replace("\001",","));
                System.out.println("list add......");
            }
        }
        System.out.println("sort sucess!");
        return list;
    }
    public static void sortedToWrite(String outputPath, List<String> list) throws IOException {
        File file = new File(outputPath);
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        for (String ub : list) {
            //String line = ub.getUid()+"\001"+ub.getBehavior()+"\001"+ub.getAid()+"\001"+ub.getBehaviorTime();
            bw.write(ub);
            bw.newLine();
        }
        bw.close();
        System.out.println("写入文件成功");

    }
    public static void sortedToWriteObject(String outputPath, List<UserBehavior> list) throws IOException {
        File file = new File(outputPath);
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        for (UserBehavior ub : list) {
            String line = ub.getUid()+"\001"+ub.getBehavior()+"\001"+ub.getAid()+"\001"+ub.getBehaviorTime();
            bw.write(line);
            bw.newLine();
        }
        bw.close();
        System.out.println("写入文件成功");

    }

    public static List<String> readFile(String filePath, int offset,
                                        String timeInterval) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String line = "";
        int lineNum = 0;// 读到了第几行
        // String stopTime = "2015-01-01 00:00:40.0";// 停止时间
        List<String> result = new LinkedList<String>();
        File file = new File(filePath);
        try {
            fis = new FileInputStream(file);
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);

            String stopTime = Config.stoptime;
            stopTime = DateTools.addDateMinut(stopTime, 1, timeInterval);
            System.out.println("stop time: " + stopTime);
            Config.stoptime = stopTime;
            result.clear();
            while (true) {

                lineNum++;

                if (lineNum < Config.offset) {// 就是说这个偏移量以前的数据已经消费了
                    line = br.readLine();
                    continue;
                }
                line = br.readLine();
                if (line == null) { // 如果读到了文件的末尾
                    result.add(null);
                    return result;
                }

//                if ((line.split("\001").length == 4) && (sdf.parse(line.split("\001")[3]).before(sdf.parse(stopTime)))) {
//                    result.add(line);
//                }
                if ((line.split("\001").length == 4)) {
                    result.add(line);
                }else {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }


    public static String fillNaN(String line, int fileLen) {

        String[] perLine = new String[fileLen];//存储有空的数据行
        String[] insert = line.split("\001");
        for (int j = 0; j < insert.length; j++) {
            perLine[j] = insert[j];
        }

        if (insert.length < fileLen) {
            for (int i = insert.length; i < fileLen; i++) {
                perLine[i] = "无";
            }
        }

        String insertLine = perLine[0];
        for (int i = 1; i < fileLen; i++) {
            insertLine += "\001" + perLine[i];
        }
        return insertLine;
    }

    public static void main(String[] args) throws IOException, SQLException {
        // MysqlDao.dropTable("userEdu");
//        MysqlDao.creatTable("userEdu");
//        read("./data/userEdu");

    }
}
