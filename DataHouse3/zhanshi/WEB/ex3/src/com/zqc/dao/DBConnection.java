package com.zqc.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBConnection {

    private static Connection connection = null;
    public DBConnection(){}
    public static Connection getConnection(){
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //  System.out.println("load");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        /*String url = ConfigRead.getProperty("url");
        String userName = ConfigRead.getProperty("username");
        String password = ConfigRead.getProperty("password");*/
        String url= "jdbc:mysql://172.31.42.214:3306/ex3";
        String userName = "root";
        String password = "cluster";
        try {
            connection = DriverManager.getConnection(url,userName,password);
            // System.out.println("sucess connect");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
    public static void closeConnection(){
        if(connection !=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public  static void closePrepareStatement(PreparedStatement ps){
        if(ps!=null){
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) {
        getConnection();
    }
}
