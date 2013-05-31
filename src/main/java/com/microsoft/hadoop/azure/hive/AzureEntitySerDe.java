package com.microsoft.hadoop.azure.hive;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.*;

public class AzureEntitySerDe extends AbstractSerDe {

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		return blob;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return null;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void initialize(Configuration conf, Properties properties)
			throws SerDeException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		// TODO Auto-generated method stub
		return null;
	}
}
