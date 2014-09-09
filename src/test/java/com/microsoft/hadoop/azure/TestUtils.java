package com.microsoft.hadoop.azure;

import java.net.*;
import java.sql.*;
import java.util.*;

import com.microsoft.windowsazure.storage.*;
import com.microsoft.windowsazure.storage.table.*;

public class TestUtils {
	public static CloudTable createTable(CloudTableClient tableClient)
			throws URISyntaxException, StorageException {
		UUID guid = UUID.randomUUID();
		String tableName = "t" + guid.toString().replace('-', 'd');
		CloudTable t = tableClient.getTableReference(tableName);
		t.create();
		return t;
	}

	public static CloudTableClient createTableClient() throws URISyntaxException {
		String accountName = getAccountName();
		String accountKey = getAccountKey();
		if (accountName == null || accountKey == null) {
			System.out.println("Please set the system properties " +
					"test.account.name and test.account.key.");
			return null;
		}
		StorageCredentials creds =
				new StorageCredentialsAccountAndKey(accountName, accountKey);
		CloudTableClient tableClient =
				new CloudTableClient(getAccountUri(), creds);
		return tableClient;
	}

	public static Connection connectToHive()
			throws ClassNotFoundException, SQLException {
		if (getHiveHost() == null || getHivePort() == null) {
			System.out.println("Please set the system properties " +
					"test.hive.host and test.hive.port.");
			return null;
		}
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		return DriverManager.getConnection(getHiveJdbcUrl());
	}

	public static String getAccountKey() {
		return System.getProperty("test.account.key");
	}

	public static URI getAccountUri() throws URISyntaxException {
		return new URI(String.format(
				"http://%s.table.core.windows.net",
				getAccountName()));
	}

	public static String getAccountName() {
		return System.getProperty("test.account.name");
	}

	public static String getHiveHost() {
		return System.getProperty("test.hive.host");
	}

	public static String getHivePort() {
		return System.getProperty("test.hive.port");
	}

	public static String getHiveJdbcUrl() {
		return "jdbc:hive2://" + getHiveHost() + ":" + getHivePort();
	}

	public static DynamicTableEntity newEntity(String partitionKey, String rowKey) {
		HashMap<String, EntityProperty> properties =
				new HashMap<String, EntityProperty>();
		DynamicTableEntity ret = new DynamicTableEntity(properties);
		ret.setPartitionKey(partitionKey);
		ret.setRowKey(rowKey);
		return ret;
	}

	public static String getCreateAzureTableSql(CloudTable table,
			String hiveTableName, String columnsDefinition)
					throws URISyntaxException {
		return "CREATE EXTERNAL TABLE " + hiveTableName +
				" (" + columnsDefinition + ") " +
				" STORED BY 'com.microsoft.hadoop.azure.hive.AzureTableHiveStorageHandler'" +
				" TBLPROPERTIES(" +
				"\"azure.table.name\"=\"" + table.getName() + "\"," +
				"\"azure.table.account.uri\"=\"" + getAccountUri() + "\"," +
				"\"azure.table.storage.key\"=\"" + getAccountKey() + "\")";
	}

	public static void insertRow(CloudTable t, String partitionKey, String rowKey)
			throws Exception{
		t.execute(TableOperation.insert(newEntity(partitionKey, rowKey)));
	}
}
