package com.microsoft.hadoop.azure.partitioners;

import java.util.*;

import org.apache.hadoop.conf.Configuration;

import com.microsoft.hadoop.azure.*;
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.table.*;

/**
 * A partitioner class that helps partition tables produced by
 * the Azure Diagnostics logger, which creates its partition keys
 * based on the tick count (100-nanoseconds intervals since epoch)
 * of each event. By default this partitions the table into 30-minute
 * chunks, though that is configurable.
 */
public class AzureDiagnosticsLogsPartitioner extends BaseAzureTablePartitioner {
	public static enum ConfigurationKeys {
		// The interval - in seconds - that every split will span.
		INTERVAL_PER_SPLIT("azure.table.logpartitioner.split.interval.seconds");

		private final String key;

		ConfigurationKeys(String key) {
			this.key = key;
		}

		public String getKey() {
			return key;
		}
	}

	private long splitIntervalSeconds = 30L * 60L;

	@Override
	public void configure(Configuration config) {
		splitIntervalSeconds = config.getLong(
				ConfigurationKeys.INTERVAL_PER_SPLIT.key,
				splitIntervalSeconds);
		if (splitIntervalSeconds <= 0) {
			throw new IllegalArgumentException(
					ConfigurationKeys.INTERVAL_PER_SPLIT.key + " should be a positive integer");
		}
	}

	@Override
	public List<AzureTableInputSplit> getSplits(CloudTable table) throws StorageException {
		ArrayList<AzureTableInputSplit> ret = new ArrayList<AzureTableInputSplit>();
		List<String> partitionKeys = getPartitionKeysAtIntervals(table);
		if (partitionKeys.size() == 0) {
			return ret;
		}
		for (int i = 0; i < partitionKeys.size() - 1; i++) {
			ret.add(new PartitionKeyRangeInputSplit(
					partitionKeys.get(i), partitionKeys.get(i + 1)));
		}
		ret.add(new PartitionKeyRangeInputSplit(
				partitionKeys.get(partitionKeys.size() - 1), null));
		return ret;
	}

	/**
	 * Gets the distinct partition keys in the given table with the wanted intervals between them.
	 * @param table The table to query.
	 * @return The list of distinct partition keys.
	 * @throws StorageException
	 */
	List<String> getPartitionKeysAtIntervals(CloudTable table)
			throws StorageException {
		TableQuery<DynamicTableEntity> getNextKeyQuery =
				getFirstRowNoFields();
		TableEntity currentEntity;
		ArrayList<String> ret = new ArrayList<String>();
		while ((currentEntity = GetSingleton(table.execute(getNextKeyQuery))) != null) {
			ret.add(currentEntity.getPartitionKey());
			long currentTicks = Long.parseLong(currentEntity.getPartitionKey());
			// Convert from ticks (100-nanosecond intervals) to seconds (divide by 10^7)
			long currentSeconds = currentTicks / 10000000;
			// Get the next wanted partition key
			long nextSeconds = currentSeconds + splitIntervalSeconds;
			long nextTicks = nextSeconds * 10000000;
			String nextPartitionKey = "0" + nextTicks;
			getNextKeyQuery = TableQuery
					.from(DynamicTableEntity.class)
					.select(new String[0])
					.where("PartitionKey ge '" + nextPartitionKey + "'")
					.take(1);
		}
		return ret;
	}
}
