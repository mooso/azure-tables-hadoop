package com.microsoft.hadoop.azure.hive;

import org.apache.hadoop.hive.serde2.objectinspector.*;

import com.microsoft.windowsazure.services.table.client.*;

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
		return inspector.getPrimitiveJavaObject(entity.getProperties().get(name));
	}
}