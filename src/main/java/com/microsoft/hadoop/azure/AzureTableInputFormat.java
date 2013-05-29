package com.microsoft.hadoop.azure;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.*;

/**
 * An input format that can read data from Azure Tables. You can
 * use it by setting it as the input format then calling
 * configureInputTable() to specify your input table. It would then
 * read the data one entity at a time: key is the row key, value
 * is the entity.
 * 
 * By default, it's partition so that every split gets the rows
 * for one partition key.
 */
public class AzureTableInputFormat
		extends InputFormat<Text, WritableEntity> {
	/**
	 * Configure the input table to be processed.
	 * @param conf The configuration for the job.
	 * @param tableName The name of the table.
	 * @param accountUri The fully qualified account URI, e.g. http://myacc.table.core.windows.net.
	 * @param storageKey The key for the Azure Storage account.
	 */
	public static void configureInputTable(Configuration conf,
			String tableName, URI accountUri, String storageKey) {
		conf.set(TABLE_NAME.getKey(), tableName);
		conf.set(ACCOUNT_URI.getKey(), accountUri.toString());
		conf.set(STORAGE_KEY.getKey(), storageKey);
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
		AzureTablePartitioner partitioner = getPartitioner(job);
		CloudTable table = getTableReference(job);
		ArrayList<InputSplit> ret = new ArrayList<InputSplit>();
		for (AzureTableInputSplit split : partitioner.getSplits(table)) {
			ret.add(split);
		}
		return ret;
	}

	private CloudTable getTableReference(Configuration job)
			throws IOException {
		CloudTableClient tableClient = createTableClient(job);
		String tableName = job.get(TABLE_NAME.getKey());
		try {
			return tableClient.getTableReference(tableName);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		} catch (StorageException e) {
			throw new IOException(e);
		}
	}

	private AzureTablePartitioner getPartitioner(Configuration job)
			throws IOException {
		Class<? extends AzureTablePartitioner> partitionerClass =
				job.getClass(PARTITIONER_CLASS.getKey(),
					DefaultTablePartitioner.class,
					AzureTablePartitioner.class);
		try {
			return partitionerClass.newInstance();
		} catch (InstantiationException e) {
			throw new IOException(e);
		} catch (IllegalAccessException e) {
			throw new IOException(e);
		}
	}

	private static CloudTableClient createTableClient(Configuration job)
			throws IOException {
		String accountUriString = job.get(ACCOUNT_URI.getKey());
		String storageKey = job.get(STORAGE_KEY.getKey());
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

	/**
	 * A record reader that will read the rows from a given input
	 * split.
	 */
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
			Configuration job = context.getConfiguration();
			CloudTableClient tableClient = createTableClient(job);
			String tableName = job.get(TABLE_NAME.getKey());
			TableQuery<WritableEntity> query =
					((AzureTableInputSplit)split).getQuery(tableName);
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
}
