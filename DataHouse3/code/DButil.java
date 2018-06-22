package DataHouse3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DButil {
    String driver="com.mysql.jdbc.Driver";
    String url="jdbc:mysql://172.31.42.214:3306/ex3";
    String user="root";
    String password="cluster";


    public Connection getConnection() {
        Connection conn=null;
        try {
            Class.forName(driver);    //加载数据库
            conn= DriverManager.getConnection(url,user,password); //和数据库建立连接
        } catch(Exception e) {
            e.printStackTrace(); //异常处理不明白，就按照这么写了
        }
        return conn;
    }

}
