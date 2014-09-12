package com.microsoft.hadoop.azure;

import java.util.*;

import org.apache.hadoop.conf.Configuration;

import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.table.*;

public interface AzureTablePartitioner {
	public void configure(Configuration config);
	public List<AzureTableInputSplit> getSplits(CloudTable table) throws StorageException;
}
