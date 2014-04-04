package com.microsoft.hadoop.azure.hive;

import org.apache.hadoop.hive.serde2.objectinspector.*;

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
		Object value = entity.getProperties().get(name);
		if (value == null) {
			// Look for it ignoring case (Hive normalizes to lower case).
			for (String key : entity.getProperties().keySet()) {
				if (key.equalsIgnoreCase(name)) {
					value = entity.getProperties().get(key);
					break;
				}
			}
			if (value == null) {
				throw new IllegalArgumentException("No property found with name " + name);
			}
		}
		return inspector.getPrimitiveJavaObject(value);
	}
}