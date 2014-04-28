package com.microsoft.hadoop.azure;

import java.util.*;

import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.table.*;

public interface AzureTablePartitioner {
	public List<AzureTableInputSplit> getSplits(CloudTable table) throws StorageException;
}
