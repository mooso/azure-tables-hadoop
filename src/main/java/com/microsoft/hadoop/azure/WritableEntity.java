package com.microsoft.hadoop.azure;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Writable;

import com.microsoft.windowsazure.storage.table.*;

/**
 * A representation of an Azure Table that implements
 * {@link org.apache.hadoop.io.Writable} so it can be processed by Hadoop. 
 */
public class WritableEntity extends DynamicTableEntity implements Writable {
	@Override
	public void readFields(DataInput input) throws IOException {
		// Read the partition and row keys.
		setPartitionKey(input.readUTF());
		setRowKey(input.readUTF());
		setTimestamp(new Date(input.readLong()));
		// Read the rest of the properties.
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
		// Write the partition and row keys.
		output.writeUTF(getPartitionKey());
		output.writeUTF(getRowKey());
		output.writeLong(getTimestamp().getTime());
		// Write the rest of the properties.
		HashMap<String, EntityProperty> properties =
				getProperties();
		output.writeInt(properties.size());
		for (Map.Entry<String, EntityProperty> current : properties.entrySet()) {
			output.writeUTF(current.getKey());
			output.writeUTF(current.getValue().getValueAsString());
		}
	}
}
