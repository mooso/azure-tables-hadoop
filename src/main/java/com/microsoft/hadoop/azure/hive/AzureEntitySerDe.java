package com.microsoft.hadoop.azure.hive;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hive.serde.serdeConstants.*;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.*;

import com.microsoft.hadoop.azure.*;

/**
 * A Hive SerDe that knows how to interpret Azure Tables entities.
 */
public class AzureEntitySerDe extends AbstractSerDe {
	// The object inspector for the entity.
	private EntityObjectInspector objectInspector;

	/**
	 * Initialize the serde for the given table.
	 */
	@Override
	public void initialize(Configuration conf, Properties tbl)
			throws SerDeException {
		// Get the column list from the table.
    String columnNameProperty = tbl.getProperty(LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(LIST_COLUMN_TYPES);
    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnTypeProperty);
    if (columnNames.size() != columnTypes.size()) {
    	throw new IllegalArgumentException(
    			"Column names list: " + columnNames +
    			" doesn't match column types list: " + columnTypes);
    }
    // Translate the columns into fields. We assume an identity mapping
    // where column names in Hive match those in the Azure Table.
    ArrayList<PrimitiveField> fields = new ArrayList<PrimitiveField>(columnNames.size());
    for (int c = 0; c < columnNames.size(); c++) {
    	fields.add(new PrimitiveField(columnNames.get(c),
    			EntityPropertyInspector.getInspectorForType(columnTypes.get(c).getTypeName())));
    }
    objectInspector = new EntityObjectInspector(fields);
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		return blob;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return objectInspector;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// No stats supported.
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return WritableEntity.class;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		return (WritableEntity)obj;
	}
}
