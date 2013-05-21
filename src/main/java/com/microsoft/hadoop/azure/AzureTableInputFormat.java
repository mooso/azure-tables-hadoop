package com.microsoft.hadoop.azure;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;

public class AzureTableInputFormat
		extends InputFormat<Text, WritableEntity> {
	private static final String TABLE_NAME = "azure.table.name";
	private static final String ACCOUNT_URI = "azure.table.account.uri";
	private static final String STORAGE_KEY = "azure.table.storage.key";

	public static void configureInputTable(Configuration conf,
			String tableName, URI accountUri, String storageKey) {
		conf.set(TABLE_NAME, tableName);
		conf.set(ACCOUNT_URI, accountUri.toString());
		conf.set(STORAGE_KEY, storageKey);
	}

	@Override
	public RecordReader<Text, WritableEntity> createRecordReader(
			InputSplit split,
			TaskAttemptContext context)
					throws IOException, InterruptedException {
		return new TableRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context)
			throws IOException,
			InterruptedException {
		Configuration job = context.getConfiguration();
		CloudTableClient tableClient = createTableClient(job);
		String tableName = job.get(TABLE_NAME);
		ArrayList<InputSplit> ret = new ArrayList<InputSplit>();
		Iterable<String> partitionKeys;
		try {
			partitionKeys = getAllPartitionKeys(tableClient.getTableReference(tableName));
		} catch (URISyntaxException e) {
			throw new IOException(e);
		} catch (StorageException e) {
			throw new IOException(e);
		}
		for (String currentPartitionKey : partitionKeys) {
			ret.add(new PartitionInputSplit(currentPartitionKey));
		}
		return ret;
	}
	
	private static DynamicTableEntity GetSingleton(Iterable<DynamicTableEntity> results) {
		for (DynamicTableEntity t : results) {
			return t;
		}
		return null;
	}
	
	private static TableQuery<DynamicTableEntity> getFirstRowNoFields(CloudTable table) {
		return TableQuery
			.from(table.getName(), DynamicTableEntity.class)
			.select(new String[0])
			.take(1);
	}

	static List<String> getAllPartitionKeys(CloudTable table) {
		TableQuery<DynamicTableEntity> getNextKeyQuery =
				getFirstRowNoFields(table);
		CloudTableClient tableClient = table.getServiceClient();
		TableEntity currentEntity;
		ArrayList<String> ret = new ArrayList<String>();
		while ((currentEntity = GetSingleton(tableClient.execute(getNextKeyQuery))) != null) {
			ret.add(currentEntity.getPartitionKey());
			getNextKeyQuery = getFirstRowNoFields(table)
					.where("PartitionKey gt '" + currentEntity.getPartitionKey() + "'");
		}
		return ret;
	}

	private static CloudTableClient createTableClient(Configuration job)
			throws IOException {
		String accountUriString = job.get(ACCOUNT_URI);
		String storageKey = job.get(STORAGE_KEY);
		URI accountUri;
		try {
			accountUri = new URI(accountUriString);
		} catch (URISyntaxException ex) {
			throw new IllegalArgumentException(
					String.format("Invalid value specified for %s: %s",
							ACCOUNT_URI, accountUriString),
					ex);
		}
		String accountName = accountUri.getAuthority().split("\\.")[0];
		StorageCredentials creds =
			new StorageCredentialsAccountAndKey(accountName, storageKey);
		return new CloudTableClient(accountUri, creds);
	}

	public static class TableRecordReader
		extends RecordReader<Text, WritableEntity> {
		private Iterator<WritableEntity> queryResults;
		private WritableEntity currentEntity;
		private Text currentKey = new Text();

		/**
		 * Called once at initialization.
		 * @param split the split that defines the range of records to read
		 * @param context the information about the task
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void initialize(InputSplit split,
				TaskAttemptContext context)
				throws IOException, InterruptedException {
			String partitionKey = ((PartitionInputSplit)split).getPartitionKey();
			Configuration job = context.getConfiguration();
			CloudTableClient tableClient = createTableClient(job);
			String tableName = job.get(TABLE_NAME);
			TableQuery<WritableEntity> query =
					TableQuery
					.from(tableName, WritableEntity.class)
					.where("PartitionKey eq '" + partitionKey + "'");
			queryResults = tableClient.execute(query).iterator();
		}

		/**
		 * Read the next key, value pair.
		 * @return true if a key/value pair was read
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public boolean nextKeyValue()
				throws IOException, InterruptedException {
			if (queryResults.hasNext()) {
				currentEntity = queryResults.next();
				currentKey.set(currentEntity.getRowKey());
				return true;
			} else {
				currentEntity = null;
				return false;
			}
		}

		/**
		 * Get the current key
		 * @return the current key or null if there is no current key
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public Text getCurrentKey()
				throws IOException, InterruptedException {
			if (currentEntity == null) {
				return null;
			}
			return currentKey;
		}
		
		/**
		 * Get the current value.
		 * @return the object that was read
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public WritableEntity getCurrentValue()
				throws IOException, InterruptedException {
			return currentEntity;
		}
		
		/**
		 * The current progress of the record reader through its data.
		 * @return a number between 0.0 and 1.0 that is the fraction of the data read
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public float getProgress()
				throws IOException, InterruptedException {
			// No idea...
			return 0.5f;
		}
		
		/**
		 * Close the record reader.
		 */
		public void close() throws IOException {
		}
	}

	public static class PartitionInputSplit extends InputSplit
			implements Writable  {
		private String partitionKey;

		PartitionInputSplit() {}

		public PartitionInputSplit(String partitionKey) {
			this.partitionKey = partitionKey;
		}
		
		public String getPartitionKey() {
			return partitionKey;
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			// TODO No idea how to get the length.
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] { "localhost" };
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, partitionKey);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			partitionKey = Text.readString(in);
		}
	}
}
