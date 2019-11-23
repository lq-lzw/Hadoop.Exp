package com.hadoop.ch06.v17034460223;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo63 {

    public static void main( String[] args ) throws Exception
    {
        String tablename = "emp17034460223";
        String rowkey = "rain";
        String[] family = {"membeer_id","address","info"};
        String column = "id";
        String[] column1 = {"age","birthday","industry"};
        String[] column2 = {"city","country"};
        String value = "31";
        String[] value1 = {"28","1990-05-01","architect"};
        String[] value2 = {"ShenZhen","China"};
        createTable();
        inseert(tablename,rowkey,column,value,column1,value1,column2,value2);
        get(tablename,rowkey,"info");
        scan(tablename);
        System.out.println( "Hello my hbase!" );
    }

    //创建列表
    private static void createTable() throws Exception {
        //用于与hbasee通信
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node1,node2,node3");
        conf.set("hbase.rootdir","hdfs://node1:8020/hbase");
        conf.set("hbase.cluster.distributed","true");
        //创建hbase的连接
        Connection connect = ConnectionFactory.createConnection(conf);
        Admin admin = connect.getAdmin();
        TableName tn = TableName.valueOf("emp17034460223");
        //table列族
        String[] family = new String[]{
                "membeer_id","address","info"
        };
        //tablee的描述，把列族加入到描述当中
        HTableDescriptor desc = new HTableDescriptor(tn);
        for(int i = 0;i < family.length; i++){
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if(admin.tableExists(tn)){
            //已存在提示
            System.out.println("table Exists!");
            System.exit(0);
        }else{
            //不存在则创建
            admin.createTable((desc));
            System.out.println("create table Sucess!");
        }
    }

    //插入数据
    private static void inseert(String tablename,String rowkey,String column,String value,String[] column1,String[] value1,String[] colum2,String[] value2) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node1,node2,node3");
        conf.set("hbase.rootdir","hdfs://node1:8020/hbase");
        conf.set("hbase.cluster.distributed","true");
        //创建hbase的连接
        Connection connect = ConnectionFactory.createConnection(conf);
        //get table
        TableName tn = TableName.valueOf(tablename);
        Table table = connect.getTable(tn);
        //create put
        Put put = new Put(Bytes.toBytes(rowkey));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
        //each put
        for(int i=0;i<columnFamilies.length; i++){
            String f = columnFamilies[i].getNameAsString();
            if(f.equals("member_id")){
                for(int j = 0 ; j < column1.length ; j++){
                    put.addColumn(Bytes.toBytes(f),Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if(f.equals("info")){
                for(int j = 0 ; j < column1.length ; j++){
                    put.addColumn(Bytes.toBytes(f),Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if(f.equals("address")){
                for(int j = 0 ; j < column1.length ; j++){
                    put.addColumn(Bytes.toBytes(f),Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }

            //put data
            table.put(put);
            System.out.println("add data sucess");
        }
    }

    //查询数据并打印到控制台
    private static void get(String tablename,String rowkey,String familyname) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node1,node2,node3");
        conf.set("hbase.rootdir","hdfs://node1:8020/hbase");
        conf.set("hbase.cluster.distributed","true");
        //创建hbase的连接
        Connection connect = ConnectionFactory.createConnection(conf);

        Table table = connect.getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addFamily(Bytes.toBytes(familyname));
        Result result = table.get(get);

        for(Cell cell : result.listCells()){
            System.out.println("--------------------------------------");
            System.out.println("           satrt to get!        ");
            System.out.println("rowkey : "+new String(CellUtil.cloneRow(cell)));
            System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("column : " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
            System.out.println("timest : " + cell.getTimestamp());
        }
    }

    //扫描数据表数据并将查询结果打印到控制台
    private static void scan(String tablename) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node1,node2,node3");
        conf.set("hbase.rootdir","hdfs://node1:8020/hbase");
        conf.set("hbase.cluster.distributed","true");
        //创建hbase的连接
        Connection connect = ConnectionFactory.createConnection(conf);

        Scan scan = new Scan();
        ResultScanner rs = null;
        Table table = connect.getTable(TableName.valueOf(tablename));
        try{
            rs = table.getScanner(scan);
            for(Result r:rs){
                for(Cell cell : r.listCells()){
                    System.out.println("-------------------------------");
                    System.out.println("scan start");
                    System.out.println("rowkey : "+new String(CellUtil.cloneRow(cell)));
                    System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
                    System.out.println("column : " + new String(CellUtil.cloneFamily(cell)));
                    System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
                    System.out.println("timest : " + cell.getTimestamp());
                }
            }
        }finally {
            rs.close();
        }

    }
}
