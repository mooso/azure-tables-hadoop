package com.microsoft.hadoop.azure;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.microsoft.windowsazure.services.table.client.TableQuery;

public abstract class AzureTableInputSplit extends InputSplit
		implements Writable {

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException {
		 // Since we're pulling the data from Azure Tables, it's not localized
		 // to any single node in the cluster so just return localhost.
		 return new String[] { "localhost" };
	}

	public abstract TableQuery<WritableEntity> getQuery(String tableName);
}
