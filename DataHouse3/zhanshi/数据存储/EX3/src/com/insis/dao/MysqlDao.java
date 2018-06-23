package com.insis.dao;

import com.insis.utils.ConnectMysql;
import com.insis.utils.FileLengh;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author ZhuQichao
 * @create 2018/6/7 10:18
 **/
public class MysqlDao {
    public MysqlDao() {

    }

    public static void dropTable(String tableName) throws SQLException {
        Connection conn = ConnectMysql.getConnection();
        String dropSql = "DROP TABLE IF EXISTS " + tableName + " ;";
        Statement statement = (Statement) conn.createStatement();
        try {

            statement.executeUpdate(dropSql);//如果存在表就删除
            System.out.println("drop table " + tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            ConnectMysql.closeConnection();

        }
    }

    public static void creatTable(String tableName) throws SQLException {
        Connection conn = ConnectMysql.getConnection();
        PreparedStatement ps = null;
        String ziduan = FileLengh.valueOf(tableName).getZd();
        String[] str = ziduan.split(",");
        /*for (int i = 0; i < str.length; i++) {
            str[i] = str[i] + " VARCHAR(21800) NOT NULL ";
        }*/
        str[0] = str[0] + " VARCHAR(20) NOT NULL ";
        str[1] = str[1] + " VARCHAR(21000) NOT NULL ";
        String zd = str[0];
        for (int i = 1; i < str.length; i++) {
            zd += "," + str[i];
        }

        String dropSql = "DROP TABLE IF EXISTS " + tableName + " ;";
        Statement statement = (Statement) conn.createStatement();
        try {

            statement.executeUpdate(dropSql);//如果存在表就删除
            System.out.println("drop table " + tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String creatSql = "CREATE TABLE " + tableName + " ( " + zd + " )charset=utf8";
        System.out.println(creatSql);
        try {

            ps = conn.prepareStatement(creatSql);
            ps.executeUpdate();
            System.out.println("成功创建表： " + tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectMysql.closeConnection();
            ConnectMysql.closePrepareStatement(ps);
        }

    }

    public static void insertPerData(String tableName, String values,Connection conn) {

        PreparedStatement ps = null;
        String ziduan = FileLengh.valueOf(tableName).getZd();
        String[] valueList = values.split("\001");
        String insert = "?";
        for (int i = 1; i < ziduan.split(",").length; i++) {
            insert += ",?";
        }
        String insertSql = "insert into " + tableName + " ( " + ziduan + " ) " + "values ( " + insert + " )";
       // System.out.println("insertSql : " + insertSql);
        try {

            //conn.setAutoCommit(false);
            ps = conn.prepareStatement(insertSql);
            for (int i = 0; i < valueList.length; i++) {
                ps.setString(i + 1, valueList[i]);
                //ps.addBatch();
            }
            // ps.executeBatch();
            //conn.commit();
            ps.executeUpdate();
            System.out.println("insert success");
        } catch (SQLException e) {
            e.printStackTrace();
        }
/*        finally {
            ConnectMysql.closeConnection();
            ConnectMysql.closePrepareStatement(ps);
        }*/

    }

    public static void main(String[] args) throws SQLException {
        creatTable("test");

    }
}
