package com.microsoft.hadoop.azure.hive;

import java.sql.Timestamp;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hive.serde.serdeConstants.*;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.junit.*;


import com.microsoft.hadoop.azure.WritableEntity;
import com.microsoft.hadoop.azure.AzureTableConfiguration.Keys;
import com.microsoft.windowsazure.storage.table.*;

import static org.junit.Assert.*;

public class TestAzureEntitySerDe {

	@Test
	@SuppressWarnings("serial")
	public void testReadSimpleEntity() throws Exception {
		WritableEntity entity = new WritableEntity();
		entity.setProperties(new HashMap<String, EntityProperty>() {{
			put("intField", new EntityProperty(3));
			put("stringField", new EntityProperty("hi"));
			put("bigIntField", new EntityProperty(123L));
			put("doubleField", new EntityProperty(32.5));
			put("booleanField", new EntityProperty(false));
		}});
		entity.setRowKey("rowKey");
		entity.setPartitionKey("partKey");
		Calendar dateToStore = Calendar.getInstance();
		dateToStore.set(1980, 10, 1);
		entity.setTimestamp(dateToStore.getTime());
		AzureEntitySerDe serDe = new AzureEntitySerDe();
		Properties tbl = new Properties();
		tbl.put(LIST_COLUMNS,
				"PartitionKey,RowKey,Timestamp,intField,stringField,bigIntField,doubleField,booleanField");
		tbl.put(LIST_COLUMN_TYPES,
				"string,string,timestamp,int,string,bigint,double,boolean");
		serDe.initialize(new Configuration(), tbl);
		StructObjectInspector inspector = (StructObjectInspector)serDe.getObjectInspector();
		assertEquals("rowKey", inspector.getStructFieldData(entity,
				inspector.getStructFieldRef("RowKey")));
		assertEquals("partKey", inspector.getStructFieldData(entity,
				inspector.getStructFieldRef("PartitionKey")));
		assertEquals(new Timestamp(dateToStore.getTime().getTime()),
				inspector.getStructFieldData(entity,
						inspector.getStructFieldRef("Timestamp")));
		assertEquals(3, inspector.getStructFieldData(entity,
				inspector.getStructFieldRef("intField")));
		assertEquals("hi", inspector.getStructFieldData(entity,
				inspector.getStructFieldRef("stringField")));
		assertEquals(123L, inspector.getStructFieldData(entity,
				inspector.getStructFieldRef("bigIntField")));
		assertEquals(32.5, inspector.getStructFieldData(entity,
				inspector.getStructFieldRef("doubleField")));
		assertEquals(false, inspector.getStructFieldData(entity,
				inspector.getStructFieldRef("booleanField")));
	}

	@Test
	public void testPropertyNotFoundAndRequired() throws Exception {
		testPropertyNotFound(true);
	}

	@Test
	public void testPropertyNotFoundAndNotRequired() throws Exception {
		testPropertyNotFound(false);
	}

	@SuppressWarnings("serial")
	public void testPropertyNotFound(boolean requireFieldExists) throws Exception {
		WritableEntity entity = new WritableEntity();
		entity.setProperties(new LinkedHashMap<String, EntityProperty>() {{
			put("a", new EntityProperty(7));
			put("b", new EntityProperty("hello"));
		}});
		AzureEntitySerDe serDe = new AzureEntitySerDe();
		Properties tbl = new Properties();
		tbl.put(LIST_COLUMNS, "a,b,c");
		tbl.put(LIST_COLUMN_TYPES, "int,string,int");
		Configuration conf = new Configuration();
		if (requireFieldExists) {
			conf.setBoolean(Keys.REQUIRE_FIELD_EXISTS.getKey(), true);
		}
		serDe.initialize(conf, tbl);
		StructObjectInspector inspector = (StructObjectInspector)serDe.getObjectInspector();
		assertEquals(7, inspector.getStructFieldData(entity,
				inspector.getStructFieldRef("a")));
		try {
			Object returned = inspector.getStructFieldData(entity,
					inspector.getStructFieldRef("c"));
			if (requireFieldExists) {
				fail("Should've thrown here.");
			} else {
				assertNull(returned);
			}
		} catch (IllegalArgumentException ex) {
			if (requireFieldExists) {
				assertEquals("No property found with name c. Properties found: a,b",
						ex.getMessage());
			} else {
				throw ex;
			}
		}
	}
}
