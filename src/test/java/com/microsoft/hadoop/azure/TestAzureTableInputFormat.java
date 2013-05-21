package com.microsoft.hadoop.azure;

import java.net.*;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.junit.*;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;

import static com.microsoft.hadoop.azure.AzureTableInputFormat.*;

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

	@Test
	public void testGetSplits() throws Exception {
		CloudTableClient tableClient = createTableClient();
		assumeNotNull(tableClient);
		t = createTable(tableClient);
		Configuration conf = new Configuration();
		AzureTableInputFormat.configureInputTable(conf,
				t.getName(), getAccountUri(), getAccountKey());
		AzureTableInputFormat inputFormat = new AzureTableInputFormat();
		JobContext jobContext = new JobContext(conf, new JobID("jt", 5));
		List<InputSplit> obtainedSplits = inputFormat.getSplits(jobContext);
		assertEquals(0, obtainedSplits.size());
		insertRow(t, "p1", "r1");
		obtainedSplits = inputFormat.getSplits(jobContext);
		assertEquals(1, obtainedSplits.size());
		assertEquals("p1", ((PartitionInputSplit)obtainedSplits.get(0)).getPartitionKey());
		insertRow(t, "p1", "r2");
		insertRow(t, "p2", "r1");
		insertRow(t, "p3", "r1");
		obtainedSplits = inputFormat.getSplits(jobContext);
		assertEquals(3, obtainedSplits.size());
		assertEquals("p1", ((PartitionInputSplit)obtainedSplits.get(0)).getPartitionKey());
		assertEquals("p2", ((PartitionInputSplit)obtainedSplits.get(1)).getPartitionKey());
		assertEquals("p3", ((PartitionInputSplit)obtainedSplits.get(2)).getPartitionKey());
	}

	@Test
	public void testRecordReader() throws Exception {
		CloudTableClient tableClient = createTableClient();
		assumeNotNull(tableClient);
		t = createTable(tableClient);
		insertRow(t, "p1", "r1");
		insertRow(t, "p1", "r2");

		Configuration conf = new Configuration();
		AzureTableInputFormat.configureInputTable(conf,
				t.getName(), getAccountUri(), getAccountKey());
		AzureTableInputFormat inputFormat = new AzureTableInputFormat();
		JobContext jobContext = new JobContext(conf, new JobID("jt", 5));
		List<InputSplit> obtainedSplits = inputFormat.getSplits(jobContext);
		assertEquals(1, obtainedSplits.size());
		TaskAttemptContext taskContext =
				new TaskAttemptContext(conf, new TaskAttemptID());
		InputSplit split = obtainedSplits.get(0);
		RecordReader<Text, WritableEntity> reader =
				inputFormat.createRecordReader(split, taskContext);
		assertNotNull(reader);
		reader.initialize(split, taskContext);
		assertTrue(reader.nextKeyValue());
		assertEquals("r1", reader.getCurrentKey().toString());
		assertTrue(reader.nextKeyValue());
		assertEquals("r2", reader.getCurrentKey().toString());
		assertFalse(reader.nextKeyValue());
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

	private String getAccountKey() {
		return System.getProperty("test.account.key");
	}

	private URI getAccountUri() throws URISyntaxException {
		return new URI(String.format(
				"http://%s.table.core.windows.net",
				getAccountName()));
	}

	private String getAccountName() {
		return System.getProperty("test.account.name");
	}
}
