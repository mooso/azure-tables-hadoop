package com.microsoft.hadoop.azure;

import java.net.*;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.*;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;

public class TestAzureTableInputFormat {
	CloudTable t;
	
	@After
	public void tearDown() throws Exception {
		if (t != null) {
			t.delete();
			t = null;
		}
	}

	private static TableEntity newEntity(String partitionKey, String rowKey) {
		HashMap<String, EntityProperty> properties =
				new HashMap<String, EntityProperty>();
		DynamicTableEntity ret = new DynamicTableEntity(properties);
		ret.setPartitionKey(partitionKey);
		ret.setRowKey(rowKey);
		return ret;
	}

	private static void insertRow(CloudTable t, String partitionKey, String rowKey)
			throws Exception{
		t.getServiceClient().execute(t.getName(),
				TableOperation.insert(newEntity(partitionKey, rowKey)));
	}

	@Test
	public void testGetAllPartitionKeys() throws Exception {
		CloudTableClient tableClient = createTableClient();
		assumeNotNull(tableClient);
		t = createTable(tableClient);
		insertRow(t, "p1", "r1");
		insertRow(t, "p1", "r2");
		insertRow(t, "p2", "r1");
		insertRow(t, "p3", "r1");
		List<String> partitionKeys = AzureTableInputFormat.getAllPartitionKeys(t);
		assertEquals(3, partitionKeys.size());
		assertEquals("p1", partitionKeys.get(0));
		assertEquals("p2", partitionKeys.get(1));
		assertEquals("p3", partitionKeys.get(2));
	}

	private CloudTable createTable(CloudTableClient tableClient)
			throws URISyntaxException, StorageException {
		UUID guid = UUID.randomUUID();
		String tableName = "t" + guid.toString().replace('-', 'd');
		CloudTable t = tableClient.getTableReference(tableName);
		t.create();
		return t;
	}

	private CloudTableClient createTableClient() throws URISyntaxException {
		String accountName = System.getProperty("test.account.name");
		String accountKey = System.getProperty("test.account.key");
		if (accountName == null || accountKey == null) {
			System.out.println("Please set the system properties " +
					"test.account.name and test.account.key.");
			return null;
		}
		StorageCredentials creds =
				new StorageCredentialsAccountAndKey(accountName, accountKey);
		CloudTableClient tableClient =
				new CloudTableClient(new URI(String.format(
						"http://%s.table.core.windows.net",
						accountName)), creds);
		return tableClient;
	}
}
