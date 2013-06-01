package com.microsoft.hadoop.azure.hive;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.*;

import java.util.*;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.security.authorization.*;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.mapred.*;

import com.microsoft.hadoop.azure.oldinterface.OldAzureTableInputFormat;

public class AzureTableHiveStorageHandler
		extends Configured
		implements HiveStorageHandler {

	private static void transferProperty(final Properties tableProperties,
			final Map<String, String> jobProperties,
			Keys propertyToTransfer) {
		String value = tableProperties.getProperty(propertyToTransfer.getKey());
		if (value != null) {
			jobProperties.put(propertyToTransfer.getKey(), value);
		}
	}

	@Override
	public void configureInputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();

    transferProperty(tableProperties, jobProperties, Keys.TABLE_NAME);
    transferProperty(tableProperties, jobProperties, Keys.ACCOUNT_URI);
    transferProperty(tableProperties, jobProperties, Keys.STORAGE_KEY);
    transferProperty(tableProperties, jobProperties, Keys.PARTITIONER_CLASS);
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
		throw new NotImplementedException("Can't do output format yet.");
	}

	@SuppressWarnings("deprecation")
	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return AzureEntitySerDe.class;
	}
}
