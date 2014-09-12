package com.microsoft.hadoop.azure;

import java.util.*;

import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.table.*;

/**
 * A default partitioner for Azure Tables that splits the table by
 * putting all the rows with a given Partition Key into a split.
 */
public class DefaultTablePartitioner extends BaseAzureTablePartitioner {

	@Override
	public List<AzureTableInputSplit> getSplits(CloudTable table) throws StorageException {
		ArrayList<AzureTableInputSplit> ret = new ArrayList<AzureTableInputSplit>();
		Iterable<String> partitionKeys;
		partitionKeys = getAllPartitionKeys(table);
		for (String currentPartitionKey : partitionKeys) {
			ret.add(new PartitionInputSplit(currentPartitionKey));
		}
		return ret;
	}

	/**
	 * Gets all the distinct partition keys in the given table.
	 * @param table The table to query.
	 * @return The list of distinct partition keys.
	 * @throws StorageException 
	 */
	static List<String> getAllPartitionKeys(CloudTable table) throws StorageException {
		// Since Azure Tables (at the time of writing) doesn't expose an
		// elegant generic way to query this, I do it by querying the partition
		// key for the first row, then the next greater key, and so on until
		// I get all the keys.
		TableQuery<DynamicTableEntity> getNextKeyQuery =
				getFirstRowNoFields();
		TableEntity currentEntity;
		ArrayList<String> ret = new ArrayList<String>();
		while ((currentEntity = GetSingleton(table.execute(getNextKeyQuery))) != null) {
			ret.add(currentEntity.getPartitionKey());
			getNextKeyQuery = getFirstRowNoFields()
					.where("PartitionKey gt '" + currentEntity.getPartitionKey() + "'");
		}
		return ret;
	}
}
