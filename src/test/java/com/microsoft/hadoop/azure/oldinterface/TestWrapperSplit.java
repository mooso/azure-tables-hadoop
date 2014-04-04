package com.microsoft.hadoop.azure.oldinterface;

import java.io.*;
import com.microsoft.hadoop.azure.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapred.JobConf;
import org.junit.*;
import static org.junit.Assert.*;

public class TestWrapperSplit {
	@Test
	public void testSerialization() throws Exception {
		PartitionInputSplit split = new PartitionInputSplit("pk1");
		byte[] serialized;
		{
			// Create a wrapper and serialize it to bytes.
			WrapperSplit wrapper = new WrapperSplit(split,
					new Path("dummy"), new JobConf());
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(outStream);
			wrapper.write(dataOut);
			dataOut.close();
			serialized = outStream.toByteArray();
		}
		{
			// Now deserialize and see that we get the same values.
			ByteArrayInputStream inStream = new ByteArrayInputStream(serialized);
			SerializationFactory sf = new SerializationFactory(new Configuration());
			Deserializer<WrapperSplit> des = sf.getDeserializer(WrapperSplit.class);
			des.open(inStream);
			WrapperSplit obtained = des.deserialize(null);
			des.close();
			assertEquals("pk1", ((PartitionInputSplit)obtained.getWrappedSplit()).getPartitionKey());
			assertEquals("dummy", obtained.getPath().toString());
		}
	}
}
