package com.pigudf;
import java.io.IOException; 
import java.util.ArrayList; 
import java.util.List; 
 
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.HBaseConfiguration; 
import org.apache.hadoop.hbase.HColumnDescriptor; 
import org.apache.hadoop.hbase.HTableDescriptor; 
import org.apache.hadoop.hbase.KeyValue; 
import org.apache.hadoop.hbase.MasterNotRunningException; 
import org.apache.hadoop.hbase.ZooKeeperConnectionException; 
import org.apache.hadoop.hbase.client.Delete; 
import org.apache.hadoop.hbase.client.Get; 
import org.apache.hadoop.hbase.client.HBaseAdmin; 
import org.apache.hadoop.hbase.client.HTable; 
import org.apache.hadoop.hbase.client.HTablePool; 
import org.apache.hadoop.hbase.client.Put; 
import org.apache.hadoop.hbase.client.Result; 
import org.apache.hadoop.hbase.client.ResultScanner; 
import org.apache.hadoop.hbase.client.Scan; 
import org.apache.hadoop.hbase.filter.Filter; 
import org.apache.hadoop.hbase.filter.FilterList; 
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter; 
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp; 
import org.apache.hadoop.hbase.util.Bytes; 
 
public class HbaseTest { 
 
    public static Configuration conf; 
    static { 
        conf = HBaseConfiguration.create(); 
     // configuration.set("hbase.zookeeper.quorum", "ip-172-31-21-148.ap-southeast-2.compute.internal");
     // configuration.set("hbase.zookeeper.property.clientPort", "2181");
       // conf.set("hadoop.security.authentication", "Kerberos");
	//conf.set("hbase.security.authentication", "Kerberos");
//	conf.set("hbase.master.kerberos.principal", "hbase/ip-172-31-17-88.ap-southeast-2.compute.internal@test.com");
//	conf.set("hbase.zookeeper.quorum", "ip-172-31-17-88.ap-southeast-2.compute.internal");
//	conf.set("hbase.zookeeper.property.clientPort", "2181");
	//conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-secure");
        //conf.addResource("/etc/hbase/conf/hbase-site.xml");
	//conf.addResource("/etc/hadoop/conf/core-site.xml");
	//conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("hadoop.security.authentication", "kerberos");
        //conf.set("hbase.rpc.protection ", "privacy");
        //conf.set("hbase.cluster.distributed", "true");

        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@test.com");
        conf.set("hbase.master.keytab.file", "/etc/hbase.keytab");
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@test.com");
        conf.set("hbase.regionserver.keytab.file", "/etc/hbase.keytab");
        // for   kerberos setting
        //conf.set("hbase.master.kerberos.principal", "hbase/_HOST@test.com");
        //conf.set("hbase.master.keytab.file", "/etc/hbase.keytab");
        //conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@test.com");
        //conf.set("hbase.regionserver.keytab.file", "/etc/hbase.keytab");
        //System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
        //System.setProperty("sun.security.krb5.debug","false");

    } 
 
    public static void main(String[] args) { 
         createTable("wujintao"); 
         //insertData("wujintao"); 
        // QueryAll("wujintao"); 
        // QueryByCondition1("wujintao"); 
        // QueryByCondition2("wujintao"); 
        //QueryByCondition3("wujintao"); 
        //deleteRow("wujintao","abcdef"); 
        //deleteByCondition("wujintao","abcdef"); 
    } 
 
     
    public static void createTable(String tableName) { 
        System.out.println("start create table ......"); 
	//UserGroupInformation.setConfiguration(conf);
        try { 
	    //UserGroupInformation.loginUserFromKeytab("hbase/ip-172-31-17-88.ap-southeast-2.compute.internal@test.com", "/etc/hbase.keytab");
            HBaseAdmin hBaseAdmin = new HBaseAdmin(conf); 
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建 
                hBaseAdmin.disableTable(tableName); 
                hBaseAdmin.deleteTable(tableName); 
                System.out.println(tableName + " is exist,detele...."); 
            } 
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName); 
            tableDescriptor.addFamily(new HColumnDescriptor("column1")); 
            tableDescriptor.addFamily(new HColumnDescriptor("column2")); 
            tableDescriptor.addFamily(new HColumnDescriptor("column3")); 
            hBaseAdmin.createTable(tableDescriptor); 
        } catch (MasterNotRunningException e) { 
            e.printStackTrace(); 
        } catch (ZooKeeperConnectionException e) { 
            e.printStackTrace(); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
        System.out.println("end create table ......"); 
    } 
 
     
    public static void insertData(String tableName) { 
        System.out.println("start insert data ......"); 
        HTablePool pool = new HTablePool(conf, 1000); 
        HTable table = (HTable) pool.getTable(tableName); 
        Put put = new Put("112233bbbcccc".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值 
        put.add("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列 
        put.add("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列 
        put.add("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列 
        try { 
            table.put(put); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
        System.out.println("end insert data ......"); 
    } 
 
     
    public static void dropTable(String tableName) { 
        try { 
            HBaseAdmin admin = new HBaseAdmin(conf); 
            admin.disableTable(tableName); 
            admin.deleteTable(tableName); 
        } catch (MasterNotRunningException e) { 
            e.printStackTrace(); 
        } catch (ZooKeeperConnectionException e) { 
            e.printStackTrace(); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
 
    } 
     
     public static void deleteRow(String tablename, String rowkey)  { 
        try { 
            HTable table = new HTable(conf, tablename); 
            List list = new ArrayList(); 
            Delete d1 = new Delete(rowkey.getBytes()); 
            list.add(d1); 
             
            table.delete(list); 
            System.out.println("删除行成功!"); 
             
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
         
 
    } 
 
      
     public static void deleteByCondition(String tablename, String rowkey)  { 
            //目前还没有发现有效的API能够实现根据非rowkey的条件删除这个功能能，还有清空表全部数据的API操作 
 
    } 
 
 
     
    public static void QueryAll(String tableName) { 
        HTablePool pool = new HTablePool(conf, 1000); 
        HTable table = (HTable) pool.getTable(tableName); 
        try { 
            ResultScanner rs = table.getScanner(new Scan()); 
            for (Result r : rs) { 
                System.out.println("获得到rowkey:" + new String(r.getRow())); 
                for (KeyValue keyValue : r.raw()) { 
                    System.out.println("列：" + new String(keyValue.getFamily()) 
                            + "====值:" + new String(keyValue.getValue())); 
                } 
            } 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    } 
 
     
    public static void QueryByCondition1(String tableName) { 
 
        HTablePool pool = new HTablePool(conf, 1000); 
        HTable table = (HTable) pool.getTable(tableName); 
        try { 
            Get scan = new Get("abcdef".getBytes());// 根据rowkey查询 
            Result r = table.get(scan); 
            System.out.println("获得到rowkey:" + new String(r.getRow())); 
            for (KeyValue keyValue : r.raw()) { 
                System.out.println("列：" + new String(keyValue.getFamily()) 
                        + "====值:" + new String(keyValue.getValue())); 
            } 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    } 
 
     
    public static void QueryByCondition2(String tableName) { 
 
        try { 
            HTablePool pool = new HTablePool(conf, 1000); 
            HTable table = (HTable) pool.getTable(tableName); 
            Filter filter = new SingleColumnValueFilter(Bytes 
                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes 
                    .toBytes("aaa")); // 当列column1的值为aaa时进行查询 
            Scan s = new Scan(); 
            s.setFilter(filter); 
            ResultScanner rs = table.getScanner(s); 
            for (Result r : rs) { 
                System.out.println("获得到rowkey:" + new String(r.getRow())); 
                for (KeyValue keyValue : r.raw()) { 
                    System.out.println("列：" + new String(keyValue.getFamily()) 
                            + "====值:" + new String(keyValue.getValue())); 
                } 
            } 
        } catch (Exception e) { 
            e.printStackTrace(); 
        } 
 
    } 
 
     
    public static void QueryByCondition3(String tableName) { 
 
        try { 
            HTablePool pool = new HTablePool(conf, 1000); 
            HTable table = (HTable) pool.getTable(tableName); 
 
            List<Filter> filters = new ArrayList<Filter>(); 
 
            Filter filter1 = new SingleColumnValueFilter(Bytes 
                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes 
                    .toBytes("aaa")); 
            filters.add(filter1); 
 
            Filter filter2 = new SingleColumnValueFilter(Bytes 
                    .toBytes("column2"), null, CompareOp.EQUAL, Bytes 
                    .toBytes("bbb")); 
            filters.add(filter2); 
 
            Filter filter3 = new SingleColumnValueFilter(Bytes 
                    .toBytes("column3"), null, CompareOp.EQUAL, Bytes 
                    .toBytes("ccc")); 
            filters.add(filter3); 
 
            FilterList filterList1 = new FilterList(filters); 
 
            Scan scan = new Scan(); 
            scan.setFilter(filterList1); 
            ResultScanner rs = table.getScanner(scan); 
            for (Result r : rs) { 
                System.out.println("获得到rowkey:" + new String(r.getRow())); 
                for (KeyValue keyValue : r.raw()) { 
                    System.out.println("列：" + new String(keyValue.getFamily()) 
                            + "====值:" + new String(keyValue.getValue())); 
                } 
            } 
            rs.close(); 
 
        } catch (Exception e) { 
            e.printStackTrace(); 
        } 
 
    } 
 
}
