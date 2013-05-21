package com.microsoft.hadoop.azure;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Writable;

import com.microsoft.windowsazure.services.table.client.*;

public class WritableEntity extends DynamicTableEntity implements Writable {
	@Override
	public void readFields(DataInput input) throws IOException {
		setPartitionKey(input.readUTF());
		setRowKey(input.readUTF());
		int numProperties = input.readInt();
		HashMap<String, EntityProperty> properties =
				new HashMap<String, EntityProperty>();
		for (int i = 0; i < numProperties; i++) {
			properties.put(input.readUTF(),
					new EntityProperty(input.readUTF()));
		}
		setProperties(properties);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(getPartitionKey());
		output.writeUTF(getRowKey());
		HashMap<String, EntityProperty> properties =
				getProperties();
		output.writeInt(properties.size());
		for (Map.Entry<String, EntityProperty> current : properties.entrySet()) {
			output.writeUTF(current.getKey());
			output.writeUTF(current.getValue().getValueAsString());
		}
	}
}
