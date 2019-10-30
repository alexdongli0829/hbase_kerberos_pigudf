package com.pigudf;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTestNew 
{
    public static void main(String[] args) throws IOException
    {
        HBaseConfiguration hc = new HBaseConfiguration(new Configuration());
        hc.set("hbase.security.authentication", "kerberos");
        hc.set("hadoop.security.authentication", "kerberos");
        //hc.set("hadoop.rpc.protection ", "privacy");
        //hc.set("hbase.cluster.distributed", "true");

        // for  kerberos setting
        hc.set("hbase.master.kerberos.principal", "hbase/_HOST@test.com");
        hc.set("hbase.master.keytab.file", "/etc/hbase.keytab");
        hc.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@test.com");
        hc.set("hbase.regionserver.keytab.file", "/etc/hbase.keytab");
        //System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
        //System.setProperty("sun.security.krb5.debug","false");

        TableName tableName = TableName.valueOf("junk");
        HTableDescriptor ht = new HTableDescriptor(tableName);

        ht.addFamily( new HColumnDescriptor("education"));
        ht.addFamily( new HColumnDescriptor("projects"));
        System.out.println( "connecting" );
        Connection connection = ConnectionFactory.createConnection(hc);
        Admin hba = connection.getAdmin();

        System.out.println( "Creating Table" );
        hba.createTable( ht );
        
        Table socuretable = connection.getTable(TableName.valueOf("junk"));
        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("education"), Bytes.toBytes("school"),Bytes.toBytes("ValueOneForPut1Qual1"));
        socuretable.put(put1);
        socuretable.close();
        System.out.println("Done......");
    }
}
