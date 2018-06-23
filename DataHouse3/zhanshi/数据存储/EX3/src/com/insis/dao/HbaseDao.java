package com.insis.dao;

import com.insis.utils.ConfigRead;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.util.*;

/**
 * @author ZhuQichao
 * @create 2018/6/7 10:21
 **/
public class HbaseDao {
    public HbaseDao(){

    }
    public static Configuration configuration;
    public static org.apache.hadoop.hbase.client.Connection connection;
    public static Admin admin;
    public static void init() {
        configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum", "172.31.42.152,172.31.42.87,172.31.42.214");
        configuration.set("hbase.zookeeper.quorum",ConfigRead.getProperty("hbase.zookeeper.quorum"));
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            System.out.println("初始化成功");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static org.apache.hadoop.hbase.client.Connection getConnection() throws IOException {
        configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum", "172.31.42.152,172.31.42.87,172.31.42.214");
        configuration.set("hbase.zookeeper.quorum",ConfigRead.getProperty("hbase.zookeeper.quorum"));
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection(configuration);
        return conn;



    }

    public static void close() {
        if (admin != null) {
            try {
                admin.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (null != connection) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("关闭连接成功");
    }

    public static void createTable(String tableName, String[] fileds) throws IOException {

        init();
        TableName tableName1 = TableName.valueOf(tableName);
        if (admin.tableExists(tableName1)) {
            System.out.println("table exists!");
            deleteTable(tableName);
        }
        Set<String> colFamily = new HashSet<>();
        for (int i = 1; i < fileds.length; i++) {
            colFamily.add(fileds[i].split(":")[0]);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName1);
        for (Iterator<String> it = colFamily.iterator(); it.hasNext(); ) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(it.next().toString());
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        System.out.println("create " + tableName + " success!");

    }

    //fileds a:c,a:d
    public static void addRecord(String tableName, String row, String[] fileds, String[] values) throws IOException {
        HbaseDao.init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < fileds.length; i++) {
            String colFamily = fileds[i].split(":")[0];
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(fileds[i].split(":")[1]), Bytes.toBytes(values[i]));
            puts.add(put);
        }
        table.put(puts);
        System.out.println("insert row : " + row + " success!");
        table.close();
    }
    public static void addRecordCon(String tableName, String row, String[] fileds, String[] values,org.apache.hadoop.hbase.client.Connection con) throws IOException {

        Table table = con.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < fileds.length; i++) {
            String colFamily = fileds[i].split(":")[0];
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(fileds[i].split(":")[1]), Bytes.toBytes(values[i]));
            puts.add(put);
        }
        table.put(puts);
        System.out.println("insert row : " + row + " success!");
        table.close();
    }
    public static void scanColumn(String tableName, String row, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(column.split(":")[0]), Bytes.toBytes(column.split(":")[1]));
        ResultScanner resultScanner = table.getScanner(scan);
        long count = 0;
        for (Result r : resultScanner) {
            System.out.println("Scan Row : " + row);
            for (KeyValue kv : r.list()) {

                System.out.println("colfamily : " + Bytes.toString(kv.getFamily()));
                System.out.println("col : " + Bytes.toString(kv.getQualifier()));
                if (kv.getValueLength() == 0) {
                    System.out.println("value : " + "null");
                } else {
                    System.out.println("value : " + Bytes.toString(kv.getValue()));
                }

                count++;
            }
            System.out.println("the total scan number is : " + count);
            System.out.println("Scan end ！ ");
        }
        table.close();
        System.out.println();

    }

    public static void scanTable(String tableName) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result r : resultScanner) {
            if (r.isEmpty()) {
                System.out.println("NO data Now ");
            } else {
                for (KeyValue kv : r.list()) {
                    System.out.println("Scan Row : " + Bytes.toString(kv.getRow()));
                    System.out.println("colfamily : " + Bytes.toString(kv.getFamily()));
                    System.out.println("col : " + Bytes.toString(kv.getQualifier()));
                    if (kv.getValueLength() == 0) {
                        System.out.println("value : " + "null");
                    } else {
                        System.out.println("value : " + Bytes.toString(kv.getValue()));
                    }
                }
            }
        }

        System.out.println("Scan end ！ ");
        table.close();
        System.out.println();

    }
    public static void scanTableCon(String tableName,org.apache.hadoop.hbase.client.Connection con) throws IOException {

        Table table = con.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result r : resultScanner) {
            if (r.isEmpty()) {
                System.out.println("NO data Now ");
            } else {
                for (KeyValue kv : r.list()) {
                    System.out.println("Scan Row : " + Bytes.toString(kv.getRow()));
                    System.out.println("colfamily : " + Bytes.toString(kv.getFamily()));
                    System.out.println("col : " + Bytes.toString(kv.getQualifier()));
                    if (kv.getValueLength() == 0) {
                        System.out.println("value : " + "null");
                    } else {
                        System.out.println("value : " + Bytes.toString(kv.getValue()));
                    }
                }
            }
        }

        System.out.println("Scan end ！ ");
        table.close();
        System.out.println();

    }
    public static void deleteTable(String tableName) {
        try {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));

                admin.deleteTable(TableName.valueOf(tableName));
                System.out.println("delete " + tableName + " success!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String tableName = ConfigRead.getProperty("hbase.namespace") + ":" + "testNamespace";
        String[] f = {"a:1","a:2"};
        String[] v = {"a1","a2"};
        try {
            createTable(tableName,f);

            addRecord(tableName,"t",f,v);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
