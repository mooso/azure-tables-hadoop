package com.microsoft.hadoop.azure.partitioners;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;

import com.google.common.base.Joiner;
import com.microsoft.hadoop.azure.*;
import com.microsoft.windowsazure.storage.table.*;
import com.microsoft.windowsazure.storage.table.TableQuery.QueryComparisons;

public class PartitionKeyRangeInputSplit extends AzureTableInputSplit {
	private String partitionKeyStart;
	private String partitionKeyEnd;

	PartitionKeyRangeInputSplit() {}

	public PartitionKeyRangeInputSplit(String partitionKeyStart,
			String partitionKeyEnd) {
		this.partitionKeyStart = partitionKeyStart;
		this.partitionKeyEnd = partitionKeyEnd;
	}

	public String getPartitionKeyStart() {
		return partitionKeyStart;
	}

	public String getPartitionKeyEnd() {
		return partitionKeyEnd;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		writeNullableString(out, partitionKeyStart);
		writeNullableString(out, partitionKeyEnd);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		partitionKeyStart = readNullableString(in);
		partitionKeyEnd = readNullableString(in);
	}

	@Override
	public TableQuery<WritableEntity> getQuery() {
		TableQuery<WritableEntity> query = TableQuery
				.from(WritableEntity.class);
		List<String> clauses = new ArrayList<String>();
		if (partitionKeyStart != null) {
			clauses.add(TableQuery.generateFilterCondition("PartitionKey",
					QueryComparisons.GREATER_THAN_OR_EQUAL,
					partitionKeyStart));
		}
		if (partitionKeyEnd != null) {
			clauses.add(TableQuery.generateFilterCondition("PartitionKey",
					QueryComparisons.LESS_THAN,
					partitionKeyEnd));
		}
		if (clauses.size() > 0) {
			query = query.where(Joiner.on(" and ").join(clauses));
		}
		return query;
	}

	private static void writeNullableString(DataOutput out, String s)
			throws IOException {
		out.writeBoolean(s != null);
		if (s != null) {
			Text.writeString(out, s);
		}
	}

	private static String readNullableString(DataInput in)
			throws IOException {
		boolean stringExists = in.readBoolean();
		if (stringExists) {
			return Text.readString(in);
		} else {
			return null;
		}
	}
}
