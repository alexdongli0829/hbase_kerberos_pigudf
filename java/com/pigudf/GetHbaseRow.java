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
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * udf test
 */
public class GetHbaseRow extends EvalFunc<String> {

	/** The Constant DEFAULT_VALUE. */
	private static final String DEFAULT_VALUE = "NO_VALUE";
	private static final String ERROR_VALUE = "ERROR_VALUE";

	/** The tgt table. */
	private Table tgtTable = null;

	@Override
	public String exec(final Tuple input) throws IOException {
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
			return ERROR_VALUE;
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
	private Table getHTable(final String tgtTableName) throws IOException, InterruptedException {
		if (tgtTable == null) {
		Connection connection = getConnection();

		TableName tableName = TableName.valueOf(tgtTableName);
		tgtTable = connection.getTable(tableName);

		}
		return tgtTable;
	}

	public Connection getConnection() throws IOException {
		Logger.getRootLogger().setLevel(Level.DEBUG);
		Configuration configuration = HBaseConfiguration.create();

		// Just need one hbase-site.xml which will help to add all necessary property into the configuration
		configuration.addResource("/etc/hbase/conf/hbase-site.xml");
		//according to https://docs.oracle.com/javase/1.5.0/docs/guide/security/jgss/tutorials/Troubleshooting.html, look like a bug in jdk1.8
		System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
		Connection connection = ConnectionFactory.createConnection(configuration);
		return connection;
	}

}
