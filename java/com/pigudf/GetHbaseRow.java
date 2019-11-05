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
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * This UDF can be called from Pig Script as follows: a = load 'input' using
 * PigStorage() as (c1:chararray); c1 will be used as RowKey b = foreach a
 * generate c1, existsRow("Table",c1); -- Here Table is the nameo of table to
 * search.
 */
public class GetHbaseRow extends EvalFunc<String> {

	/** The Constant DEFAULT_VALUE. */
	private static final String DEFAULT_VALUE = "NO_VALUE";
	private static final String ERROR_VALUE = "ERROR_VALUE";

	/** The tgt table. */
	private Table tgtTable = null;

	@Override
	public String exec(final Tuple input) throws IOException {
//		UserGroupInformation.loginUserFromKeytab("hbase/ip-172-31-23-0.ap-southeast-2.compute.internal@test.com", "/etc/hbase.keytab");
		if (input == null || input.size() == 0 || (String) input.get(0) == null) {
			return DEFAULT_VALUE;
		}
		try {
			final String rowKey = (String) input.get(0);
			final String tgtTableName = (String) input.get(1);
			final String columnFamilyName = (String) input.get(2);
			final String columnName = (String) input.get(3);
			final Table table = getHTable(tgtTableName);
			final Get get = new Get(Bytes.toBytes(rowKey.trim()));
			get.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));
			final Result result = table.get(get);
			final String res = new String(result.value());
			return res;
		} catch (final Exception e) {
			getLogger().info(e.getMessage());
			getLogger().info("errror happpeeeeeeeeing!!!!!!!!!!!!!!!");
			return e.getMessage();
		}
	}

	@Override
	public void finish() {
		super.finish();
		if (tgtTable != null) {
			try {
				tgtTable.close();
			} catch (final IOException e) {
				getLogger().info(e.getMessage());
			}
		}
	}

	/**
	 * Gets the h table.
	 *
	 * @param tgtTableName the tgt table name
	 * @param zkq          the zkq
	 * @return the h table
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("deprecation")
	//private Table getHTable(final String tgtTableName, final String zkq) throws IOException, InterruptedException {
	private Table getHTable(final String tgtTableName) throws IOException, InterruptedException {
		if (tgtTable == null) {

                Configuration configuration = HBaseConfiguration.create();
                Logger.getRootLogger().setLevel(Level.DEBUG);


                //configuration.set("hadoop.security.authentication", "kerberos");
                
               // configuration.set("hbase.security.authentication", "kerberos");
                //configuration.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@test.com");
                //configuration.set("hbase.master.kerberos.principal", "hbase/_HOST@test.com");
                //configuration.set("hbase.rpc.protection ", "privacy");
//              configuration.set("hbase.master.keytab.file", "/etc/hbase.keytab");
//              configuration.set("hbase.regionserver.keytab.file", "/etc/hbase.keytab");
//              configuration.set("hadoop.rpc.protection ", "privacy");
//
//
//
                // Zookeeper quorum
                configuration.addResource("/etc/hbase/conf/hbase-site.xml");
		System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
		System.setProperty("sun.security.krb5.debug","true");

		System.out.println("first test:"+UserGroupInformation.getCurrentUser());

                //configuration.set("hbase.zookeeper.quorum", "ip-172-31-26-188.ap-southeast-2.compute.internal");
                //configuration.set("hbase.zookeeper.property.clientPort", "2181");

		UserGroupInformation.setConfiguration(configuration);
        	UserGroupInformation.loginUserFromKeytab("hbase/ip-172-31-26-188.ap-southeast-2.compute.internal@test.com", "/etc/hbase.keytab");
		//Connection connection = getConnection();
		System.out.println("current user second:"+UserGroupInformation.getCurrentUser());
		System.out.println("login user second:"+UserGroupInformation.getLoginUser());
		System.out.println("if current user has kerberos:"+UserGroupInformation.getCurrentUser().hasKerberosCredentials());
		UserGroupInformation.getCurrentUser().setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
		UserGroupInformation.getCurrentUser().reloginFromKeytab();
		System.out.println("if login user has kerberos:"+UserGroupInformation.getLoginUser().hasKerberosCredentials());
		System.out.println("if current user has kerberos after:"+UserGroupInformation.getCurrentUser().hasKerberosCredentials());
        	//UserGroupInformation.checkTGTAndReloginFromKeytab();
		//System.out.println("if has kerberos second:"+UserGroupInformation.getCurrentUser().hasKerberosCredentials());

		Connection connection = ConnectionFactory.createConnection(configuration); 

		TableName tableName = TableName.valueOf(tgtTableName);
		tgtTable = connection.getTable(tableName);

		}
		return tgtTable;
	}
/**	
	public Connection getConnection() throws IOException {
		
		Logger.getRootLogger().setLevel(Level.DEBUG);
		Configuration configuration = HBaseConfiguration.create();
		

		//configuration.set("hadoop.security.authentication", "kerberos");
		configuration.set("hbase.security.authentication", "kerberos");
		configuration.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@test.com"); 
		configuration.set("hbase.master.kerberos.principal", "hbase/_HOST@test.com");
		configuration.set("hbase.rpc.protection ", "privacy");
//		configuration.set("hbase.master.keytab.file", "/etc/hbase.keytab");
//		configuration.set("hbase.regionserver.keytab.file", "/etc/hbase.keytab"); 
//		configuration.set("hadoop.rpc.protection ", "privacy");
//
//
//
		// Zookeeper quorum

		configuration.set("hbase.zookeeper.quorum", "ip-172-31-26-188.ap-southeast-2.compute.internal");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
//                configuration.set("hbase.master", "172.31.17.88:16000");

//		System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
 //       	System.setProperty("sun.security.krb5.debug","true");
		//UserGroupInformation.setConfiguration(configuration);
	//	UserGroupInformation.loginUserFromKeytab("hbase/ip-172-31-23-0.ap-southeast-2.compute.internal@test.com", "/etc/hbase.keytab");

		Connection connection = ConnectionFactory.createConnection(configuration);
		return connection;
	}
***/
}
