package com.microsoft.hadoop.azure.partitioners;

import static com.microsoft.hadoop.azure.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

import org.junit.*;

import com.microsoft.hadoop.azure.*;
import com.microsoft.windowsazure.storage.table.*;

public class TestAzureDiagnosticsLogsPartitioner {
	CloudTable t;

	@After
	public void tearDown() throws Exception {
		if (t != null) {
			t.delete();
			t = null;
		}
	}

	private static String getPartitionKeyForMinute(double minuteNumber) {
		long baseTicks = 635454394200000000L;
		long ticksPerMinutes = 60L * 10 * 1000 * 1000;
		long wantedTicks = baseTicks + (long)(ticksPerMinutes * minuteNumber);
		return "0" + wantedTicks;
	}

	@Test
	public void testGetPartitionKeysAtIntervals() throws Exception {
		CloudTableClient tableClient = createTableClient();
		assumeNotNull(tableClient);
		t = createTable(tableClient);
		insertRow(t, getPartitionKeyForMinute(0), "r1");
		insertRow(t, getPartitionKeyForMinute(0), "r2");
		insertRow(t, getPartitionKeyForMinute(1), "r1");
		insertRow(t, getPartitionKeyForMinute(40), "r1");
		AzureDiagnosticsLogsPartitioner partitioner = new AzureDiagnosticsLogsPartitioner();
		List<String> partitionKeys = partitioner.getPartitionKeysAtIntervals(t);
		assertEquals(2, partitionKeys.size());
		assertEquals(getPartitionKeyForMinute(0), partitionKeys.get(0));
		assertEquals(getPartitionKeyForMinute(40), partitionKeys.get(1));
	}

	private static void printResultSet(ResultSet rs) throws Exception {
		ResultSetMetaData metadata = rs.getMetaData();
		for (int i = 0; i < metadata.getColumnCount(); i++)
			System.out.printf("|%s", metadata.getColumnName(i + 1));
		System.out.println("|");
		int numRows = 0;
		while (rs.next()) {
			for (int i = 0; i < metadata.getColumnCount(); i++)
				System.out.printf("|%s", rs.getObject(i + 1));
			System.out.println();
			numRows++;
		}
		if (rs.getWarnings() != null) {
			rs.getWarnings().printStackTrace();
		}
		System.out.println("Total rows: " + numRows);
	}

	@Test
	public void testSelectCountStar() throws Exception {
		CloudTableClient tableClient = createTableClient();
		assumeNotNull(tableClient);
		Connection connection = connectToHive();
		assumeNotNull(connection);
		try {
			t = createTable(tableClient);
			// Insert 50 rows for the first 5 minutes.
			for (int minute = 0; minute < 5; minute++) {
				for (int row = 0; row < 10; row++) {
					insertRow(t, getPartitionKeyForMinute(minute), "r" + row);
				}
			}
			// Insert 5 rows for at the 55th minute.
			for (int row = 0; row < 5; row++) {
				insertRow(t, getPartitionKeyForMinute(55), "r" + row);
			}
			// Insert 20 rows for the 200-210 minute range.
			for (int minute = 200; minute < 210; minute++) {
				for (int row = 0; row < 2; row++) {
					insertRow(t, getPartitionKeyForMinute(minute), "r" + row);
				}
			}
			AzureDiagnosticsLogsPartitioner partitioner = new AzureDiagnosticsLogsPartitioner();
			List<AzureTableInputSplit> partitionKeys = partitioner.getSplits(t);
			assertEquals(3, partitionKeys.size());
			connection.createStatement().execute("DROP TABLE IF EXISTS WadSelectCountStarTest");
			connection.createStatement().execute(
					getCreateAzureTableSql(t,
							"WadSelectCountStarTest", "partitionkey string, rowkey string",
							AzureDiagnosticsLogsPartitioner.class.getName()));
			ResultSet rs = connection.createStatement().executeQuery(
					"SELECT COUNT(*) FROM WadSelectCountStarTest");
			assertTrue(rs.next());
			try {
				assertEquals(75, rs.getInt(1));
			} catch (AssertionError ex) {
				printResultSet(connection.createStatement().executeQuery(
						"SELECT * FROM WadSelectCountStarTest"));
				throw ex;
			}
			assertFalse(rs.next());
		} finally {
			connection.close();
		}
	}
}
