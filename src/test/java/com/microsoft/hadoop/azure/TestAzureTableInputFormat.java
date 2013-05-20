package com.microsoft.hadoop.azure;

import java.net.*;
import java.util.*;

import static org.junit.Assert.*;
import org.junit.*;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;

public class TestAzureTableInputFormat {
	final String accountUri = "http://mostry.table.core.windows.net/";
	final String accountKey =
			"mZyONtAn5w/B+ExPVc+B1ra5d8zMfpTlitpym52nKbJXS4XK6oMcA5LbEh//AiTY0dZXTdsnItMfaXyuuYKQqw==";
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
		StorageCredentials creds =
				new StorageCredentialsAccountAndKey("mostry", accountKey);
		CloudTableClient tableClient =
				new CloudTableClient(new URI(accountUri), creds);
		return tableClient;
	}
}
