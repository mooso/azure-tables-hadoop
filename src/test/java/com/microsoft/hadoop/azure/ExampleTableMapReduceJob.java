package com.microsoft.hadoop.azure;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import com.microsoft.windowsazure.services.table.client.*;

import static com.microsoft.hadoop.azure.TestUtils.*;

public class ExampleTableMapReduceJob extends Configured
	implements Tool {

	private static TableEntity newEntity(String partitionKey,
			String rowKey, long data) {
		HashMap<String, EntityProperty> properties =
				new HashMap<String, EntityProperty>();
		properties.put("data", new EntityProperty(data));
		DynamicTableEntity ret = new DynamicTableEntity(properties);
		ret.setPartitionKey(partitionKey);
		ret.setRowKey(rowKey);
		return ret;
	}

	private static void populate(CloudTable t)
			throws Exception{
		for (int partition = 0; partition < 5; partition ++) {
			TableBatchOperation batch = new TableBatchOperation();
			for (int row = 0; row < 100; row++) {
				batch.add(TableOperation.insert(newEntity(
						"p" + partition,
						"r" + row,
						row * 10)));				
			}
			t.getServiceClient().execute(t.getName(), batch);
		}
	}

	public static class DataMapper
		extends Mapper<Text, WritableEntity, Text, Text> {
		private final Text partition = new Text();

		@Override
		protected void map(Text key, WritableEntity value, Context context)
				throws IOException, InterruptedException {
			long currentData = value.getProperties().get("data").getValueAsLong();
			if (currentData == 50) {
				partition.set(value.getPartitionKey());
				context.write(partition, key);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		CloudTableClient tableClient = createTableClient();
		if (tableClient == null) {
			return 1;
		}
		CloudTable t = createTable(tableClient);
		try {
			System.out.println("Populating data...");
			populate(t);
			System.out.println("Done!");
			AzureTableInputFormat.configureInputTable(getConf(), t.getName(),
					getAccountUri(), getAccountKey());
			Job job = new Job(getConf());
			job.setMapperClass(DataMapper.class);
			job.setInputFormatClass(AzureTableInputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path("/tableOut"));
			job.submit();
			job.waitForCompletion(true);
		} finally {
			t.delete();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ExampleTableMapReduceJob(), args);
	}
}
