package com.microsoft.hadoop.azure.hive;

import static org.apache.hadoop.hive.serde.serdeConstants.*;

import java.util.*;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.*;

import com.microsoft.windowsazure.services.table.client.EntityProperty;

public abstract class EntityPropertyInspector implements PrimitiveObjectInspector {
	@SuppressWarnings("serial")
	private static final Map<String, EntityPropertyInspector> typeToInspector =
    Collections.unmodifiableMap(new HashMap<String, EntityPropertyInspector>() {{
//        put(STRING_TYPE_NAME, javaStringObjectInspector);
//        put(TINYINT_TYPE_NAME, javaByteObjectInspector);
//        put(SMALLINT_TYPE_NAME, javaShortObjectInspector);
        put(INT_TYPE_NAME, new IntEntityPropertyInspector());
//        put(BIGINT_TYPE_NAME, javaLongObjectInspector);
//        put(FLOAT_TYPE_NAME, javaFloatObjectInspector);
//        put(DOUBLE_TYPE_NAME, javaDoubleObjectInspector);
//        put(BOOLEAN_TYPE_NAME, javaBooleanObjectInspector);
//        put(DECIMAL_TYPE_NAME, javaHiveDecimalObjectInspector);
    }});
	
	public static EntityPropertyInspector getInspectorForType(String typeName)
			throws SerDeException  {
		EntityPropertyInspector ret = typeToInspector.get(typeName);
    if (ret != null) {
    	return ret;
    } else {
       throw new SerDeException("Type " + typeName + " not allowed.");
     }
	}

	@Override
	public Category getCategory() {
		return Category.PRIMITIVE;
	}

	@Override
	public boolean preferWritable() {
		return false;
	}

	@Override
	public Object copyObject(Object o) {
		return o;
	}

	@Override
	public final Object getPrimitiveJavaObject(Object property) {
		return getPrimitiveJavaObject((EntityProperty)property);
	}

	@Override
	public final Object getPrimitiveWritableObject(Object property) {
		return getPrimitiveWritableObject((EntityProperty)property);
	}

	public abstract Object getPrimitiveJavaObject(EntityProperty property);
	public abstract Object getPrimitiveWritableObject(EntityProperty property);

	public static class IntEntityPropertyInspector extends EntityPropertyInspector {
		@Override
		public Class<?> getJavaPrimitiveClass() {
			return Integer.TYPE;
		}

		@Override
		public PrimitiveCategory getPrimitiveCategory() {
			return PrimitiveCategory.INT;
		}

		@Override
		public Class<?> getPrimitiveWritableClass() {
			return IntWritable.class;
		}

		@Override
		public String getTypeName() {
			return INT_TYPE_NAME;
		}

		@Override
		public Object getPrimitiveJavaObject(EntityProperty property) {
			return property.getValueAsInteger();
		}

		@Override
		public Object getPrimitiveWritableObject(EntityProperty property) {
			return new IntWritable(property.getValueAsInteger());
		}
	}
}
