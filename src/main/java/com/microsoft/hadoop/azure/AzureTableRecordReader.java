package com.microsoft.hadoop.azure;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.microsoft.windowsazure.services.table.client.*;

/**
 * A record reader that will read the rows from a given input
 * split.
 */
public class AzureTableRecordReader
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
		String tableName = getTableName(job);
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
