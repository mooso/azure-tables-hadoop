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
        put(STRING_TYPE_NAME, new StringEntityPropertyInspector());
        put(INT_TYPE_NAME, new IntEntityPropertyInspector());
        put(BIGINT_TYPE_NAME, new LongEntityPropertyInspector());
        put(DOUBLE_TYPE_NAME, new DoubleEntityPropertyInspector());
        put(BOOLEAN_TYPE_NAME, new BooleanEntityPropertyInspector());

        // Consider adding support for {tinyint, smallint, float, decimal}
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

	public static class StringEntityPropertyInspector extends EntityPropertyInspector {
		@Override
		public Class<?> getJavaPrimitiveClass() {
			return String.class;
		}

		@Override
		public PrimitiveCategory getPrimitiveCategory() {
			return PrimitiveCategory.STRING;
		}

		@Override
		public Class<?> getPrimitiveWritableClass() {
			return Text.class;
		}

		@Override
		public String getTypeName() {
			return STRING_TYPE_NAME;
		}

		@Override
		public Object getPrimitiveJavaObject(EntityProperty property) {
			return property.getValueAsString();
		}

		@Override
		public Object getPrimitiveWritableObject(EntityProperty property) {
			return new Text(property.getValueAsString());
		}
	}

	public static class BooleanEntityPropertyInspector extends EntityPropertyInspector {
		@Override
		public Class<?> getJavaPrimitiveClass() {
			return Boolean.TYPE;
		}

		@Override
		public PrimitiveCategory getPrimitiveCategory() {
			return PrimitiveCategory.BOOLEAN;
		}

		@Override
		public Class<?> getPrimitiveWritableClass() {
			return BooleanWritable.class;
		}

		@Override
		public String getTypeName() {
			return BOOLEAN_TYPE_NAME;
		}

		@Override
		public Object getPrimitiveJavaObject(EntityProperty property) {
			return property.getValueAsBoolean();
		}

		@Override
		public Object getPrimitiveWritableObject(EntityProperty property) {
			return new BooleanWritable(property.getValueAsBoolean());
		}
	}

	public static class LongEntityPropertyInspector extends EntityPropertyInspector {
		@Override
		public Class<?> getJavaPrimitiveClass() {
			return Long.TYPE;
		}

		@Override
		public PrimitiveCategory getPrimitiveCategory() {
			return PrimitiveCategory.LONG;
		}

		@Override
		public Class<?> getPrimitiveWritableClass() {
			return LongWritable.class;
		}

		@Override
		public String getTypeName() {
			return BIGINT_TYPE_NAME;
		}

		@Override
		public Object getPrimitiveJavaObject(EntityProperty property) {
			return property.getValueAsLong();
		}

		@Override
		public Object getPrimitiveWritableObject(EntityProperty property) {
			return new LongWritable(property.getValueAsLong());
		}
	}

	public static class DoubleEntityPropertyInspector extends EntityPropertyInspector {
		@Override
		public Class<?> getJavaPrimitiveClass() {
			return Double.TYPE;
		}

		@Override
		public PrimitiveCategory getPrimitiveCategory() {
			return PrimitiveCategory.DOUBLE;
		}

		@Override
		public Class<?> getPrimitiveWritableClass() {
			return DoubleWritable.class;
		}

		@Override
		public String getTypeName() {
			return DOUBLE_TYPE_NAME;
		}

		@Override
		public Object getPrimitiveJavaObject(EntityProperty property) {
			return property.getValueAsDouble();
		}

		@Override
		public Object getPrimitiveWritableObject(EntityProperty property) {
			return new DoubleWritable(property.getValueAsDouble());
		}
	}
}
