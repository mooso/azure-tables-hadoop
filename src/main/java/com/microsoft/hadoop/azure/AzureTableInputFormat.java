package com.microsoft.hadoop.azure;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.microsoft.windowsazure.storage.table.*;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.*;

/**
 * An input format that can read data from Azure Tables. You can
 * use it by setting it as the input format then calling
 * configureInputTable() to specify your input table. It would then
 * read the data one entity at a time: key is the row key, value
 * is the entity.
 * 
 * By default, it's partition so that every split gets the rows
 * for one partition key.
 */
public class AzureTableInputFormat
		extends InputFormat<Text, WritableEntity> {
	@Override
	public RecordReader<Text, WritableEntity> createRecordReader(
			InputSplit split,
			TaskAttemptContext context)
					throws IOException, InterruptedException {
		return new AzureTableRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context)
			throws IOException,
			InterruptedException {
		Configuration job = context.getConfiguration();
		AzureTablePartitioner partitioner = getPartitioner(job);
		CloudTable table = getTableReference(job);
		ArrayList<InputSplit> ret = new ArrayList<InputSplit>();
		for (AzureTableInputSplit split : partitioner.getSplits(table)) {
			ret.add(split);
		}
		return ret;
	}
}
