package com.pigudf;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseTestNew 
{
    public static void main(String[] args) throws IOException
    {
    
	Admin hba;
    	Connection connection;
    HBaseConfiguration hc = new HBaseConfiguration(new Configuration());
    //    below is the minimum setting for hbase client with kerberos. Meanwhile, the java need to run with user who has kerberos key
        hc.addResource("/home/ec2-user/hbase_kerberos_pigudf/conf/hbase-site.xml");
	hc.set("hbase.zookeeper.quorum", "ip-172-31-17-43.ap-southeast-2.compute.internal");
	hc.set("hbase.zookeeper.property.clientPort", "2181");
	hc.set("hbase.security.authentication", "kerberos");
	hc.set("hbase.rpc.protection ", "privacy");
        hc.set("hbase.client.retries.number", "1");
	hc.set("hadoop.security.authentication", "kerberos");
        //hc.set("ipc.socket.timeout", "10");
        //hc.set("hbase.master.kerberos.principal", "hbase/_HOST@test.com");
        hc.set("hbase.master.kerberos.principal", "hbase/_HOST@test.com");
        hc.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@test.com");
        //hc.set("hbase.cluster.distributed", "true");
	
//	hc.set("keytab.file" , "/home/ec2-user/hbase_kerberos_pigudf/conf/ec2-user.keytab" );
             // 这个可以理解成用户名信息，也就是Principal
 //       hc.set("kerberos.principal" , "ec2-user/ip-172-31-4-19.ap-southeast-2.compute.internal@test.com" );    

        System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        System.setProperty("sun.security.krb5.debug","true");
        //UserGroupInformation.setConfiguration(hc);
//             try {
 //                 UserGroupInformation.loginUserFromKeytab("euser/ip-172-31-4-19.ap-southeast-2.compute.internal@test.com","/home/ec2-user/hbase_kerberos_pigudf/conf/ec2-user.keytab" );
                  //UserGroupInformation.loginUserFromKeytab("ec2-user/ip-172-31-4-19.ap-southeast-2.compute.internal@test.com","/home/ec2-user/hbase_kerberos_pigudf/conf/ec2-user.keytab" );
          //  } catch (IOException e) {
                   // TODO Auto-generated catch block
           //       e.printStackTrace();
            //}
	 //     try {
          //      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("ec2-user/ip-172-31-4-19.ap-southeast-2.compute.internal@test.com","/home/ec2-user/hbase_kerberos_pigudf/conf/ec2-user.keytab");
           //     UserGroupInformation.setLoginUser(ugi);
	   //
	   //
	UserGroupInformation.setConfiguration(hc);
	UserGroupInformation.loginUserFromKeytab("ec2-user/ip-172-31-4-19.ap-southeast-2.compute.internal@test.com","/home/ec2-user/hbase_kerberos_pigudf/conf/ec2-user.keytab");

        System.out.println( "connecting" );
        connection = ConnectionFactory.createConnection(hc);
        hba = connection.getAdmin();
            //} catch (IOException e1) {
             //   throw new RuntimeException("Kerberos身份认证失败：" + e1.getMessage(), e1);
            //}

        // for  kerberos setting
// keytable is not ncessary for the clinet
 //       hc.set("hbase.master.keytab.file", "/etc/hbase.keytab");
   //     hc.set("hbase.regionserver.keytab.file", "/etc/hbase.keytab");

        TableName tableName = TableName.valueOf("junk");
        HTableDescriptor ht = new HTableDescriptor(tableName);

        ht.addFamily( new HColumnDescriptor("education"));
        ht.addFamily( new HColumnDescriptor("projects"));

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
