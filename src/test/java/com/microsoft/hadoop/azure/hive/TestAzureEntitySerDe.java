package com.microsoft.hadoop.azure.hive;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hive.serde.Constants.*;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.junit.*;


import com.microsoft.hadoop.azure.WritableEntity;
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
		AzureEntitySerDe serDe = new AzureEntitySerDe();
		Properties tbl = new Properties();
		tbl.put(LIST_COLUMNS, "intField,stringField,bigIntField,doubleField,booleanField");
		tbl.put(LIST_COLUMN_TYPES, "int,string,bigint,double,boolean");
		serDe.initialize(new Configuration(), tbl);
		StructObjectInspector inspector = (StructObjectInspector)serDe.getObjectInspector();
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
}
