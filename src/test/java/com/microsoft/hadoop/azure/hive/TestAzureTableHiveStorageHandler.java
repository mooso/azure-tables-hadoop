package com.microsoft.hadoop.azure.hive;

import java.util.*;

import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.junit.*;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.*;
import static org.junit.Assert.*;

public class TestAzureTableHiveStorageHandler {

	@Test
	public void testConfigureInputProperties() {
		AzureTableHiveStorageHandler handler = new AzureTableHiveStorageHandler();
		TableDesc tableDesc = new TableDesc();
		tableDesc.setProperties(new Properties());
		tableDesc.getProperties().put(Keys.TABLE_NAME.getKey(), "t");
		tableDesc.getProperties().put(Keys.ACCOUNT_URI.getKey(), "http://fakeUri");
		tableDesc.getProperties().put(Keys.STORAGE_KEY.getKey(), "fakeKey");
		Map<String, String> jobProperties = new HashMap<String, String>();
		handler.configureInputJobProperties(tableDesc, jobProperties);
		assertEquals("t", jobProperties.get(Keys.TABLE_NAME.getKey()));
		assertEquals("http://fakeUri", jobProperties.get(Keys.ACCOUNT_URI.getKey()));
		assertNull(jobProperties.get(Keys.PARTITIONER_CLASS.getKey()));
	}
}
