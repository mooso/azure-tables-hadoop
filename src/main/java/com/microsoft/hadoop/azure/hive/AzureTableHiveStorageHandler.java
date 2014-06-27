package com.microsoft.hadoop.azure.hive;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.*;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.security.authorization.*;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;

import com.microsoft.hadoop.azure.oldinterface.OldAzureTableInputFormat;

/**
 * A storage handler to connect Azure Tables to Hive.
 */
public class AzureTableHiveStorageHandler
		extends Configured
		implements HiveStorageHandler {

	/**
	 * Moves a configuration from table properties to the job configuration.
	 * @param tableProperties The table properties.
	 * @param jobProperties The job configuration.
	 * @param propertyToTransfer The property to transfer.
	 * @param required If set, then we'll throw if the property isn't specified in the
	 *                 job configuration.
	 */
	private static void transferProperty(final Properties tableProperties,
			final Map<String, String> jobProperties,
			Keys propertyToTransfer,
			boolean required) {
		String value = tableProperties.getProperty(propertyToTransfer.getKey());
		if (value != null) {
			jobProperties.put(propertyToTransfer.getKey(), value);
		} else if (required) {
			throw new IllegalArgumentException("Property " + propertyToTransfer.getKey() +
					" not found in the table properties.");
		}
	}

	/**
	 * Configure the properties for an MR job where we'll read from
	 * an Azure Table.
	 */
	@Override
	public void configureInputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();

    transferProperty(tableProperties, jobProperties, Keys.TABLE_NAME, true);
    transferProperty(tableProperties, jobProperties, Keys.ACCOUNT_URI, true);
    transferProperty(tableProperties, jobProperties, Keys.STORAGE_KEY, true);
    transferProperty(tableProperties, jobProperties, Keys.PARTITIONER_CLASS, false);
	}

	/**
	 * Configure the properties for an MR job where we'll write to
	 * an Azure Table.
	 */
	@Override
	public void configureOutputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		// Not yet implemented...
	}

	/**
	 * Deprecated - should never be called.
	 */
	@Override
	@Deprecated
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		throw new AssertionError("Shouldn't be called.");
	}

	/**
	 * No idea what this is for.
	 */
	@Override
	public HiveAuthorizationProvider getAuthorizationProvider()
			throws HiveException {
		return null;
	}

	/**
	 * Gets the input format class.
	 */
	@SuppressWarnings({ "rawtypes" })
	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return OldAzureTableInputFormat.class;
	}

	/**
	 * Option ability to hook into metadata operations.
	 */
	@Override
	public HiveMetaHook getMetaHook() {
		return null;
	}

	/**
	 * Gets the output format class, used to write to the Azure Table.
	 */
	@SuppressWarnings({ "rawtypes" })
	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		// Not yet implemented, but if we return null then the create
		// table statement will fail. Return a dummy implementation that'll
		// throw if ever actually used.
		return NotImplementedOutputFormat.class;
	}

	/**
	 * Gets the SerDe that'll interpret the values coming out the
	 * of the input format.
	 */
	@Override
	public Class<? extends AbstractSerDe> getSerDeClass() {
		return AzureEntitySerDe.class;
	}

	/**
	 * Dummy implementation of output format.
	 */
	@SuppressWarnings({ "rawtypes" })
	public static class NotImplementedOutputFormat implements HiveOutputFormat {

		@Override
		public void checkOutputSpecs(FileSystem ignored, JobConf job)
				throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public RecordWriter getRecordWriter(FileSystem ignored, JobConf job,
				String name, Progressable progress) throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public FSRecordWriter getHiveRecordWriter(
				JobConf job, Path finalOutPath, Class valueClass,
				boolean isCompressed, Properties tableProperties,
				Progressable progress) throws IOException {
			throw new NotImplementedException();
		}
		
	}

	@Override
	public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
	}
}
