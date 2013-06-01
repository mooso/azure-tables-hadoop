package com.microsoft.hadoop.azure.oldinterface;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.createTableClient;
import static com.microsoft.hadoop.azure.AzureTableConfiguration.getTableName;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import com.microsoft.hadoop.azure.*;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableQuery;

public class OldAzureTableReader implements RecordReader<Text, WritableEntity> {
	private Iterator<WritableEntity> queryResults;
	private long pos;
	
	OldAzureTableReader(WrapperSplit split, Configuration conf)
			throws IOException {
		CloudTableClient tableClient = createTableClient(conf);
		String tableName = getTableName(conf);
		TableQuery<WritableEntity> query =
				split.getWrappedSplit().getQuery(tableName);
		queryResults = tableClient.execute(query).iterator();
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public WritableEntity createValue() {
		return new WritableEntity();
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public float getProgress() throws IOException {
		// No idea.
		return 0.5f;
	}

	@Override
	public boolean next(Text key, WritableEntity value) throws IOException {
		if (queryResults.hasNext()) {
			WritableEntity obtained = queryResults.next();
			key.set(obtained.getRowKey());
			value.setPartitionKey(obtained.getPartitionKey());
			value.setRowKey(obtained.getRowKey());
			value.setProperties(obtained.getProperties());
			return true;
		} else {
			return false;
		}
	}
}
