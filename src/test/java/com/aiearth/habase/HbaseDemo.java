package com.aiearth.habase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class HbaseDemo {
    private static Connection conn;

    @Before
    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "bigdata01:2181,bigdata02:2181,bigdata03:2181");
        conf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase");
        conn = ConnectionFactory.createConnection(conf);
    }

    @Test
    public void testAdd() throws IOException {
        Table student = conn.getTable(TableName.valueOf("student"));
        // rawKey
        Put laowang = new Put(Bytes.toBytes("laowang"));
        // 列族，列，值
        laowang.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
        laowang.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("man"));
        laowang.addColumn(Bytes.toBytes("level"), Bytes.toBytes("class"), Bytes.toBytes("A"));

        student.put(laowang);
        student.close();
    }

    @Test
    public void testQuery() throws IOException {
        Table student = conn.getTable(TableName.valueOf("student"));

        Get laowang = new Get(Bytes.toBytes("laowang"));
//        laowang.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
//        laowang.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"));

        Result result = student.get(laowang);

        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            byte[] bytes = CellUtil.cloneFamily(cell);
            byte[] bytes1 = CellUtil.cloneQualifier(cell);
            byte[] bytes2 = CellUtil.cloneValue(cell);

            System.out.println("列族：" + new String(bytes) + ", 列：" + new String(bytes1) + ", 值：" + new String(bytes2));
        }

//        byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
//        System.out.println(new String(value));
        student.close();
    }

    @Test
    public void testQueryVersionsData() throws IOException {
        Table student = conn.getTable(TableName.valueOf("student"));

        Get laowang = new Get(Bytes.toBytes("laowang"));
        laowang.readAllVersions();

        Result result = student.get(laowang);

        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("age"));
        for (Cell columnCell : columnCells) {
            byte[] bytes = CellUtil.cloneValue(columnCell);
            long timestamp = columnCell.getTimestamp();
            System.out.println("值：" + new String(bytes) + ", 时间戳：" + new Date(timestamp));
        }
        student.close();
    }

    @Test
    public void testDel() throws IOException {
        Table student = conn.getTable(TableName.valueOf("student"));

        Delete laowang = new Delete(Bytes.toBytes("laowang"));
//        laowang.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));

        student.delete(laowang);
        student.close();
    }

    @Test
    public void testCreateTable() throws IOException {
        Admin admin = getAdmin();
        ColumnFamilyDescriptor familyDesc1 = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("info"))
                .setMaxVersions(3)
                .build();

        ColumnFamilyDescriptor familyDesc2 = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("level"))
                .setMaxVersions(2)
                .build();

        List<ColumnFamilyDescriptor> f = Arrays.asList(familyDesc1, familyDesc2);
        TableDescriptor desc = TableDescriptorBuilder
                .newBuilder(TableName.valueOf("test"))
                .setColumnFamilies(f)
                .build();

        admin.createTable(desc);
    }

    @Test
    public void testDelTable() throws IOException {
        Admin admin = getAdmin();
        admin.disableTable(TableName.valueOf("test"));
        admin.deleteTable(TableName.valueOf("test"));
    }

    public Admin getAdmin() throws IOException {
        return conn.getAdmin();
    }

}
