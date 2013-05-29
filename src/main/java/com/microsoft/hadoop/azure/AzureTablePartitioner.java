package com.microsoft.hadoop.azure;

import java.util.*;

import com.microsoft.windowsazure.services.table.client.CloudTable;

public interface AzureTablePartitioner {
	public List<AzureTableInputSplit> getSplits(CloudTable table);
}
