package com.microsoft.hadoop.azure.oldinterface;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;

import com.microsoft.hadoop.azure.AzureTableInputSplit;

/**
 * A split in the old mapred.* API that represents a split from Azure Tables.
 * Implemented here as a FileSplit because Hive misbehaves if you give it other
 * types of splits.
 */
@SuppressWarnings("deprecation")
public class WrapperSplit extends FileSplit implements Writable {
	private AzureTableInputSplit wrappedSplit;

	/**
	 * A parameter-less constructor for deserialization.
	 */
  public WrapperSplit() {
    super((Path) null, 0, 0, (String[]) null);
  }

  /**
   * Create a split to wrap a given Azure Tables split.
   * @param wrappedSplit The split to wrap.
   * @param file The file path for the partition in Hive (needs to match).
   * @param conf The configuration.
   */
	public WrapperSplit(AzureTableInputSplit wrappedSplit,
			Path file, JobConf conf) {
		super(file, 0, 0, conf);
		this.wrappedSplit = wrappedSplit;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		String className = in.readUTF();
		try {
			wrappedSplit = (AzureTableInputSplit) ReflectionUtils.newInstance(
					Class.forName(className), new Configuration());
		} catch (Exception e) {
			throw new IOException(e);
		}
		wrappedSplit.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(wrappedSplit.getClass().getName());
		wrappedSplit.write(out);
	}

	public AzureTableInputSplit getWrappedSplit() {
		return wrappedSplit;
	}
}
