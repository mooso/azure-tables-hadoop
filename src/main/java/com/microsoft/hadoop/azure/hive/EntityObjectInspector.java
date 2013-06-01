package com.microsoft.hadoop.azure.hive;

import java.util.*;

import org.apache.hadoop.hive.serde2.objectinspector.*;

import com.microsoft.windowsazure.services.table.client.*;

public class EntityObjectInspector extends StructObjectInspector {
	private final List<PrimitiveField> fields;

	EntityObjectInspector(List<PrimitiveField> fields) {
		this.fields = fields;
	}
	
	@Override
	public Category getCategory() {
		return Category.STRUCT;
	}

	@Override
	public String getTypeName() {
		return "entity";
	}

	@Override
	public List<? extends StructField> getAllStructFieldRefs() {
		return fields;
	}

	@Override
	public Object getStructFieldData(Object data, StructField fieldRef) {
		if (data == null) {
			return null;
		}
		return ((PrimitiveField)fieldRef).getData((DynamicTableEntity)data);
	}

	@Override
	public StructField getStructFieldRef(String fieldName) {
		for (PrimitiveField f : fields) {
			if (f.getFieldName().equals(fieldName)) {
				return f;
			}
		}
		return null;
	}

	@Override
	public List<Object> getStructFieldsDataAsList(Object data) {
		DynamicTableEntity entity = (DynamicTableEntity)data;
		ArrayList<Object> ret = new ArrayList<Object>(fields.size());
		for (PrimitiveField f : fields) {
			ret.add(f.getData(entity));
		}
		return ret;
	}
}
