package com.microsoft.hadoop.azure.hive;

import java.util.*;

import org.apache.hadoop.hive.serde2.objectinspector.*;

import com.microsoft.windowsazure.storage.table.*;

/**
 * An object inspector that knows how to interpet an Azure Tables
 * entity as a Hive struct.
 */
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

	/**
	 * Extract the data for a given field from the given entity.
	 */
	@Override
	public Object getStructFieldData(Object data, StructField fieldRef) {
		if (data == null) {
			return null;
		}
		return ((PrimitiveField)fieldRef).getData((DynamicTableEntity)data);
	}

	/**
	 * Gets the field with the given name.
	 */
	@Override
	public StructField getStructFieldRef(String fieldName) {
		for (PrimitiveField f : fields) {
			// Hive stores fields as lower case, so ignore case in comparison.
			if (f.getFieldName().equalsIgnoreCase(fieldName)) {
				return f;
			}
		}
		return null;
	}

	/**
	 * Gets all the fields in the entity as a list.
	 */
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
