package com.pigudf;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
//import org.apache.pig.EvalFunc;
//import org.apache.pig.data.Tuple;

/**
 * This UDF can be called from Pig Script as follows: a = load 'input' using
 * PigStorage() as (c1:chararray); c1 will be used as RowKey b = foreach a
 * generate c1, existsRow("Table",c1); -- Here Table is the nameo of table to
 * search.
 */
public class HbaseRow {
	private static Table getHTable(final String tgtTableName) throws IOException, InterruptedException {
	
		Table tgtTable=null;
		Connection connection = getConnection();
		TableName tableName = TableName.valueOf(tgtTableName);
		tgtTable = connection.getTable(tableName);

		return tgtTable;
	}
	
	private static Connection getConnection() throws IOException {
		
		Configuration configuration = HBaseConfiguration.create();
		
		//configuration.set("hbase.zookeeper.quorum", "ip-172-31-21-148.ap-southeast-2.compute.internal");
		//configuration.set("hbase.zookeeper.property.clientPort", "2181");
		//configuration.addResource("/etc/hbase/conf/hbase-site.xml");

        	configuration.set("hbase.security.authentication", "kerberos");
        	configuration.set("hbase.rpc.protection ", "privacy");
        	configuration.set("hbase.master.kerberos.principal", "hbase/_HOST@test.com");
        	configuration.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@test.com");
		//Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(configuration));
		Connection connection = ConnectionFactory.createConnection(configuration);
		return connection;
	}
//usage: com.pigudf.HbaseRow '1990' 'test' 'cf' 'status'
	public static void main(String[] args)  throws IOException 
    	{

		try{
			final String rowKey = (String) args[0];
			final String tgtTableName = (String) args[1];
			final String columnFamilyName = (String) args[2];
			final String columnName = (String) args[3];
			final Table table = getHTable(tgtTableName);
			final Get get = new Get(Bytes.toBytes(rowKey.trim()));
			get.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));
			final Result result = table.get(get);
			final String res = new String(result.value());
			System.out.println(res);
		} catch (final Exception e) {
			System.out.println("error:"+e.getMessage());
		}
	}

}


