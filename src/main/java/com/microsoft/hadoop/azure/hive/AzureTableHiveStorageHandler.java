package com.microsoft.hadoop.azure.hive;

import java.util.*;

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

	@Override
	public void configureInputJobProperties(TableDesc arg0,
			Map<String, String> arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void configureOutputJobProperties(TableDesc arg0,
			Map<String, String> arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	@Deprecated
	public void configureTableJobProperties(TableDesc arg0,
			Map<String, String> arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public HiveAuthorizationProvider getAuthorizationProvider()
			throws HiveException {
		// TODO Auto-generated method stub
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
		return null;
	}

	@SuppressWarnings("deprecation")
	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return AzureEntitySerDe.class;
	}
}
