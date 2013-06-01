package com.microsoft.hadoop.azure.hive;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.*;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.security.authorization.*;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;

import com.microsoft.hadoop.azure.oldinterface.OldAzureTableInputFormat;

public class AzureTableHiveStorageHandler
		extends Configured
		implements HiveStorageHandler {

	private static void transferProperty(final Properties tableProperties,
			final Map<String, String> jobProperties,
			Keys propertyToTransfer,
			boolean required) {
		String value = tableProperties.getProperty(propertyToTransfer.getKey());
		if (value != null) {
			System.out.printf("Putting: %s = %s\n", propertyToTransfer.getKey(), value);
			jobProperties.put(propertyToTransfer.getKey(), value);
		} else if (required) {
			throw new IllegalArgumentException("Property " + propertyToTransfer.getKey() +
					" not found in the table properties.");
		}
	}

	@Override
	public void configureInputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();

    transferProperty(tableProperties, jobProperties, Keys.TABLE_NAME, true);
    transferProperty(tableProperties, jobProperties, Keys.ACCOUNT_URI, true);
    transferProperty(tableProperties, jobProperties, Keys.STORAGE_KEY, true);
    transferProperty(tableProperties, jobProperties, Keys.PARTITIONER_CLASS, false);
	}

	@Override
	public void configureOutputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
	}

	@Override
	@Deprecated
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		throw new AssertionError("Shouldn't be called.");
	}

	@Override
	public HiveAuthorizationProvider getAuthorizationProvider()
			throws HiveException {
		return null;
	}

	@SuppressWarnings({ "rawtypes", "deprecation" })
	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return OldAzureTableInputFormat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return null;
	}

	@SuppressWarnings({ "rawtypes", "deprecation" })
	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return NotImplementedOutputFormat.class; // BOGUS
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return AzureEntitySerDe.class;
	}
	
	@SuppressWarnings({ "rawtypes", "deprecation" })
	public static class NotImplementedOutputFormat implements HiveOutputFormat {

		@Override
		public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
				throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public RecordWriter getRecordWriter(FileSystem arg0, JobConf arg1,
				String arg2, Progressable arg3) throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
				JobConf arg0, Path arg1, Class arg2, boolean arg3, Properties arg4,
				Progressable arg5) throws IOException {
			throw new NotImplementedException();
		}
		
	}
}
