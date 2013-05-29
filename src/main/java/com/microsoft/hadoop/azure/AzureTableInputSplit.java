package com.microsoft.hadoop.azure;

import java.io.*;

import org.apache.hadoop.mapreduce.InputSplit;

import com.microsoft.windowsazure.services.table.client.TableQuery;

public abstract class AzureTableInputSplit extends InputSplit {
	public abstract TableQuery<WritableEntity> getQuery(String tableName);
	
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// Since we're pulling the data from Azure Tables, it's not localized
		// to any single node in the cluster so just return localhost.
		return new String[] { "localhost" };
	}
}
