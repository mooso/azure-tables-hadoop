package com.microsoft.hadoop.azure.hive;

import org.apache.hadoop.hive.serde2.objectinspector.*;

import com.google.common.base.Joiner;
import com.microsoft.windowsazure.storage.table.*;

/**
 * A field in an Azure Tables entity.
 */
class PrimitiveField implements StructField {
	private final EntityPropertyInspector inspector;
	private final String name;

	public PrimitiveField(String name, EntityPropertyInspector inspector) {
		this.name = name;
		this.inspector = inspector;
	}

	@Override
	public String getFieldComment() {
		return null;
	}

	@Override
	public String getFieldName() {
		return name;
	}

	@Override
	public ObjectInspector getFieldObjectInspector() {
		return inspector;
	}

	public Object getData(DynamicTableEntity entity) {
		Object value;
		if (name.equalsIgnoreCase("RowKey")) {
			value = entity.getRowKey();
		} else if (name.equalsIgnoreCase("PartitionKey")) {
			value = entity.getPartitionKey();
		} else {
			value = entity.getProperties().get(name);
			if (value == null) {
				// Look for it ignoring case (Hive normalizes to lower case).
				for (String key : entity.getProperties().keySet()) {
					if (key.equalsIgnoreCase(name)) {
						value = entity.getProperties().get(key);
						break;
					}
				}
				if (value == null) {
					throw new IllegalArgumentException(
							"No property found with name " + name +
							". Properties found: " +
							Joiner.on(',').join(entity.getProperties().keySet()));
				}
			}
		}
		return inspector.getPrimitiveJavaObject(value);
	}
}