package com.microsoft.hadoop.azure.hive;

import static org.apache.hadoop.hive.serde.Constants.*;

import java.util.*;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
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

	public static class IntEntityPropertyInspector extends EntityPropertyInspector
			implements IntObjectInspector {
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
		public Object getPrimitiveJavaObject(Object property) {
			return ((EntityProperty)property).getValueAsInteger();
		}

		@Override
		public Object getPrimitiveWritableObject(Object property) {
			return new IntWritable(((EntityProperty)property).getValueAsInteger());
		}

		@Override
		public int get(Object value) {
			return (Integer)value;
		}
	}

	public static class StringEntityPropertyInspector extends EntityPropertyInspector
			implements StringObjectInspector {
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
		public String getPrimitiveJavaObject(Object property) {
			return ((EntityProperty)property).getValueAsString();
		}

		@Override
		public Text getPrimitiveWritableObject(Object value) {
			return new Text((String)value);
		}
	}

	public static class BooleanEntityPropertyInspector extends EntityPropertyInspector
			implements BooleanObjectInspector {
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
		public Object getPrimitiveJavaObject(Object property) {
			return ((EntityProperty)property).getValueAsBoolean();
		}

		@Override
		public Object getPrimitiveWritableObject(Object property) {
			return new BooleanWritable(((EntityProperty)property).getValueAsBoolean());
		}

		@Override
		public boolean get(Object value) {
			return (Boolean)value;
		}
	}

	public static class LongEntityPropertyInspector extends EntityPropertyInspector
			implements LongObjectInspector {
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
		public Object getPrimitiveJavaObject(Object property) {
			return ((EntityProperty)property).getValueAsLong();
		}

		@Override
		public Object getPrimitiveWritableObject(Object property) {
			return new LongWritable(((EntityProperty)property).getValueAsLong());
		}

		@Override
		public long get(Object value) {
			return (Long)value;
		}
	}

	public static class DoubleEntityPropertyInspector extends EntityPropertyInspector
			implements DoubleObjectInspector {
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
		public Object getPrimitiveJavaObject(Object property) {
			return ((EntityProperty)property).getValueAsDouble();
		}

		@Override
		public Object getPrimitiveWritableObject(Object property) {
			return new DoubleWritable(((EntityProperty)property).getValueAsDouble());
		}

		@Override
		public double get(Object value) {
			return (Double)value;
		}
	}
}
