package com.microsoft.hadoop.azure.hive;

import static org.apache.hadoop.hive.serde.serdeConstants.*;

import java.util.*;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.*;
import org.apache.hadoop.io.*;

import com.microsoft.windowsazure.storage.table.*;

/**
 * An object inspector that knows how to extract values from
 * {link com.microsoft.windowsazure.services.table.client.EntityProperty}.
 */
public abstract class EntityPropertyInspector extends AbstractPrimitiveJavaObjectInspector {
	@SuppressWarnings("serial")
	/**
	 * A map that gets the appropriate inspector for a given Hive type name.
	 */
	private static final Map<String, EntityPropertyInspector> typeToInspector =
    Collections.unmodifiableMap(new HashMap<String, EntityPropertyInspector>() {{
        put(STRING_TYPE_NAME, new StringEntityPropertyInspector());
        put(INT_TYPE_NAME, new IntEntityPropertyInspector());
        put(BIGINT_TYPE_NAME, new LongEntityPropertyInspector());
        put(DOUBLE_TYPE_NAME, new DoubleEntityPropertyInspector());
        put(BOOLEAN_TYPE_NAME, new BooleanEntityPropertyInspector());

        // Consider adding support for {tinyint, smallint, float, decimal}
    }});

	/**
	 * Gets the appropriate inspector for the given type name.
	 */
	public static EntityPropertyInspector getInspectorForType(String typeName)
			throws SerDeException  {
		EntityPropertyInspector ret = typeToInspector.get(typeName);
		if (ret != null) {
			return ret;
		} else {
			throw new SerDeException("Type " + typeName + " not allowed.");
		}
	}

	protected EntityPropertyInspector(PrimitiveTypeEntry typeEntry) {
    super(typeEntry);
  }

	@Override
	public Object getPrimitiveJavaObject(Object property) {
		if (EntityProperty.class.isInstance(property)) {
			return getPrimitiveJavaObject((EntityProperty)property);
		} else {
			return super.getPrimitiveJavaObject(property);
		}
	}

	protected abstract Object getPrimitiveJavaObject(EntityProperty property);

	public static class IntEntityPropertyInspector extends EntityPropertyInspector
			implements IntObjectInspector {
		public IntEntityPropertyInspector() {
			super(PrimitiveObjectInspectorUtils.intTypeEntry);
		}

		@Override
		public Object getPrimitiveJavaObject(EntityProperty property) {
			return property.getValueAsInteger();
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
		public StringEntityPropertyInspector() {
			super(PrimitiveObjectInspectorUtils.stringTypeEntry);
		}

		@Override
		public String getPrimitiveJavaObject(EntityProperty property) {
			return property.getValueAsString();
		}

		@Override
		public String getPrimitiveJavaObject(Object property) {
			if (EntityProperty.class.isInstance(property)) {
				return getPrimitiveJavaObject((EntityProperty)property);
			} else {
				return (String)super.getPrimitiveJavaObject(property);
			}
		}

		@Override
		public Text getPrimitiveWritableObject(Object value) {
			return new Text((String)value);
		}
	}

	public static class BooleanEntityPropertyInspector extends EntityPropertyInspector
			implements BooleanObjectInspector {
		public BooleanEntityPropertyInspector() {
			super(PrimitiveObjectInspectorUtils.booleanTypeEntry);
		}

		@Override
		public Object getPrimitiveJavaObject(EntityProperty property) {
			return property.getValueAsBoolean();
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
		public LongEntityPropertyInspector() {
			super(PrimitiveObjectInspectorUtils.longTypeEntry);
		}

		@Override
		public Object getPrimitiveJavaObject(EntityProperty property) {
			return property.getValueAsLong();
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
		public DoubleEntityPropertyInspector() {
			super(PrimitiveObjectInspectorUtils.doubleTypeEntry);
		}

		@Override
		public Object getPrimitiveJavaObject(EntityProperty property) {
			return property.getValueAsDouble();
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
