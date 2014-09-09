package com.microsoft.hadoop.azure.hive;

import java.sql.*;
import java.util.*;

import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.junit.*;

import com.microsoft.windowsazure.storage.table.*;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.*;
import static com.microsoft.hadoop.azure.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

public class TestAzureTableHiveStorageHandler {
	CloudTable t;

	@After
	public void tearDown() throws Exception {
		if (t != null) {
			t.delete();
			t = null;
		}
	}


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

	@Test
	public void testSelectStar() throws Exception {
		CloudTableClient tableClient = createTableClient();
		assumeNotNull(tableClient);
		t = createTable(tableClient);
		Connection connection = connectToHive();
		assumeNotNull(connection);
		try {
			DynamicTableEntity newEntity = newEntity("p1", "r1");
			newEntity.getProperties().put("intProp", new EntityProperty(5));
			Calendar dateToStore = Calendar.getInstance();
			dateToStore.set(1980, 10, 1);
			newEntity.getProperties().put("dateProp", new EntityProperty(dateToStore.getTime()));
			t.execute(TableOperation.insert(newEntity));
			connection.createStatement().execute("DROP TABLE IF EXISTS SelectStarTest");
			connection.createStatement().execute(
					getCreateAzureTableSql(t, "SelectStarTest", "intprop int, dateprop timestamp"));
			ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM SelectStarTest");
			assertTrue(rs.next());
			assertEquals(5, rs.getInt("selectstartest.intprop"));
			Calendar dateObtained = Calendar.getInstance();
			dateObtained.setTime(rs.getTimestamp("selectstartest.dateprop"));
			assertEquals(1980, dateObtained.get(Calendar.YEAR));
			assertFalse(rs.next());
		} finally {
			connection.close();
		}
	}
}
