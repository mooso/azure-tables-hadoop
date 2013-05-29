package com.microsoft.hadoop.azure;

import static com.microsoft.hadoop.azure.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.util.*;

import org.junit.*;

import com.microsoft.windowsazure.services.table.client.*;

public class TestDefaultTablePartitioner {
	CloudTable t;
	
	@After
	public void tearDown() throws Exception {
		if (t != null) {
			t.delete();
			t = null;
		}
	}

	@Test
	public void testGetAllPartitionKeys() throws Exception {
		CloudTableClient tableClient = createTableClient();
		assumeNotNull(tableClient);
		t = createTable(tableClient);
		insertRow(t, "p1", "r1");
		insertRow(t, "p1", "r2");
		insertRow(t, "p2", "r1");
		insertRow(t, "p3", "r1");
		List<String> partitionKeys = DefaultTablePartitioner.getAllPartitionKeys(t);
		assertEquals(3, partitionKeys.size());
		assertEquals("p1", partitionKeys.get(0));
		assertEquals("p2", partitionKeys.get(1));
		assertEquals("p3", partitionKeys.get(2));
	}

}
