package com.microsoft.hadoop.azure;

import java.io.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.microsoft.windowsazure.services.table.client.TableQuery;

@SuppressWarnings("deprecation")
public abstract class AzureTableInputSplit
		extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit {
	public abstract TableQuery<WritableEntity> getQuery(String tableName);

	@Override
	public String[] getLocations() throws IOException {
		// Since we're pulling the data from Azure Tables, it's not localized
		// to any single node in the cluster so just return localhost.
		return new String[] { "localhost" };
	}
}
