package com.microsoft.hadoop.azure.oldinterface;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.*;

import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import com.microsoft.hadoop.azure.*;
import com.microsoft.windowsazure.services.table.client.*;

/**
 * An input format using the deprecated mapred.* API for reading Azure Tables.
 * Implemented here since HiveStorageHandler uses the old API.
 */
@SuppressWarnings("deprecation")
public class OldAzureTableInputFormat implements InputFormat<Text, WritableEntity> {

	@Override
	public RecordReader<Text, WritableEntity> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		return new OldAzureTableReader((WrapperSplit)split, job);
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		AzureTablePartitioner partitioner = getPartitioner(job);
		CloudTable table = getTableReference(job);
		ArrayList<InputSplit> ret = new ArrayList<InputSplit>();
    Path [] tablePaths = FileInputFormat.getInputPaths(job);
		for (AzureTableInputSplit split : partitioner.getSplits(table)) {
			ret.add(new WrapperSplit(split, tablePaths[0], job));
		}
		return ret.toArray(new InputSplit[0]);
	}
}
