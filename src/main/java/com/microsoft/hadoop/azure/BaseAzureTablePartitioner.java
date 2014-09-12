package com.microsoft.hadoop.azure;

import java.util.*;

import org.apache.hadoop.conf.Configuration;

import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.table.*;

public abstract class BaseAzureTablePartitioner implements
		AzureTablePartitioner {

	@Override
	public void configure(Configuration config) {
	}

	@Override
	public abstract List<AzureTableInputSplit> getSplits(CloudTable table)
			throws StorageException;

	/**
	 * Gets the single entity in the given list.
	 * @param results The list of results that we know contains at most one entity.
	 * @return The single entity or null if none found.
	 */
	protected static DynamicTableEntity GetSingleton(Iterable<DynamicTableEntity> results) {
		for (DynamicTableEntity t : results) {
			return t;
		}
		return null;
	}

	/**
	 * Query for the first row in the given table and don't retrieve
	 * any actual fields beyond the always-given partition and row keys.
	 * @param table The table to query.
	 * @return The query.
	 */
	protected static TableQuery<DynamicTableEntity> getFirstRowNoFields() {
		return TableQuery
			.from(DynamicTableEntity.class)
			.select(new String[0])
			.take(1);
	}
}
