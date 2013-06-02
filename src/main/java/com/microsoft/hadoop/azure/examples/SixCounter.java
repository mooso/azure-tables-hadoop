package com.microsot.hadoop.azure.examples;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import com.microsoft.hadoop.azure.*;
import com.microsoft.windowsazure.services.core.storage.StorageCredentialsAccountAndKey;
import com.microsoft.windowsazure.services.table.client.*;

/**
 * A simple silly example to illustrate how to read data from
 * Azure Tables in a Hadoop MR job: populates random data into
 * a table, then counts how many sixes there are in this data.
 */
public class SixCounter extends Configured
	implements Tool {
	private static final long NUM_PARTITIONS = 5;
	private static final long NUM_ROWS_PER_PARTITION = 1000;
	private static final long MAX_ROWS_PER_BATCH = 100;

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
		Random rand = new Random();
		for (int partition = 0; partition < NUM_PARTITIONS; partition ++) {
			TableBatchOperation batch = new TableBatchOperation();
			int rowsInBatch = 0;
			for (int row = 0; row < NUM_ROWS_PER_PARTITION; row++) {
				batch.add(TableOperation.insert(newEntity(
						"p" + partition,
						"r" + row,
						rand.nextInt(100))));
				rowsInBatch++;
				if (rowsInBatch >= MAX_ROWS_PER_BATCH) {
					t.getServiceClient().execute(t.getName(), batch);
					batch = new TableBatchOperation();
					rowsInBatch = 0;
				}
			}
			if (rowsInBatch > 0) {
				t.getServiceClient().execute(t.getName(), batch);
			}
		}
	}

	public static class SixFinderMapper
		extends Mapper<Text, WritableEntity, IntWritable, LongWritable> {
		private static final IntWritable SIX = new IntWritable(6);
		private static final LongWritable ONE = new LongWritable(1);

		@Override
		protected void map(Text key, WritableEntity value, Context context)
				throws IOException, InterruptedException {
			long currentData = value.getProperties().get("data").getValueAsLong();
			if (currentData == 6) {
				context.write(SIX, ONE);
			}
		}
	}

	public static class TallyReducer
		extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
		private final LongWritable sum = new LongWritable();

		@Override
		protected void reduce(IntWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long currentSum = 0;
			for (LongWritable c : values) {
				currentSum += c.get();
			}
			sum.set(currentSum);
			context.write(key, sum);
		}
	}

	private String accountName;
	private String accountKey;
	private URI accountUri;

	@Override
	public int run(String[] args) throws Exception {
		CloudTable t = createTableReference(args);
		if (t == null) {
			return 1;
		}
		t.create();
		try {
			System.out.printf("Populating %d partitions each with %d rows...\n",
					NUM_PARTITIONS, NUM_ROWS_PER_PARTITION);
			populate(t);
			System.out.println("Done!");
			Path output = new Path("sixCounterOut");
			FileSystem fs = FileSystem.get(getConf());
			fs.delete(output, true);
			Job job = configureJob(t, output);
			job.submit();
			if (job.waitForCompletion(true)) {
				if (!displayResults(output, fs)) {
					return 2;
				}
			} else {
				return 3;
			}
			fs.delete(output, true);
		} finally {
			t.delete();
		}
		return 0;
	}

	private Job configureJob(CloudTable t, Path outputPath) throws IOException {
		Job job = new Job(getConf());
		AzureTableConfiguration.configureInputTable(job.getConfiguration(), t.getName(),
				accountUri, accountKey);
		job.setJarByClass(getClass());
		job.setMapperClass(SixFinderMapper.class);
		job.setCombinerClass(TallyReducer.class);
		job.setReducerClass(TallyReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(AzureTableInputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}

	private boolean displayResults(Path output, FileSystem fs) throws IOException {
		FileStatus[] reduceOutputs = fs.globStatus(new Path(output, "part-r-*"));
		if (reduceOutputs.length == 0) {
			System.err.println("No outputs found.");
			return false;
		}
		for (FileStatus singleOutput : reduceOutputs) {
			InputStream in = fs.open(singleOutput.getPath());
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
			long sum = 0;
			while ((line = reader.readLine()) != null) {
				long current = Long.parseLong(line.split("\t")[1]);
				sum += current;
			}
			reader.close();
			in.close();
			System.out.printf("There were %d sixes in the table.\n", sum);
		}
		return true;
	}

	private CloudTable createTableReference(String[] args) 
			throws Exception {
		if (args.length < 2) {
			System.out.printf(
					"Usage: hadoop jar <jarPath> %s <accountName> <accountKey>\n",
					getClass().getName());
			return null;
		}
		accountName = args[0];
		accountKey = args[1];
		String tableName = "s" +
				UUID.randomUUID().toString().replace('-', 'd');
		StorageCredentialsAccountAndKey creds =
				new StorageCredentialsAccountAndKey(accountName, accountKey);
		accountUri = new URI(String.format("http://%s.table.core.windows.net/",
				accountName));
		CloudTableClient tableClient = new CloudTableClient(accountUri, creds);
		return tableClient.getTableReference(tableName);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SixCounter(), args);
	}
}
