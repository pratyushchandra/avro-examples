package avro.example;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGeneric {
	private static Schema schema;
	private static File schemaFile;

	@BeforeClass
	public static void before() throws IOException {
		schemaFile = new File("src/main/resources/avro/users.avsc");
		schema = Utils.getSchema(schemaFile);
	}

	@Test
	public void test1() throws IOException {
		File outputFile = Misc.getNonExistingTemporaryDirectoryName();
		List<GenericRecord> inputData = new ArrayList<GenericRecord>();
		GenericRecord message1 = new GenericData.Record(schema);
		message1.put("name", "Pratyush");
		message1.put("favorite_number", 1);
		message1.put("favorite_color", "Blue");
		GenericRecord message2 = new GenericData.Record(schema);
		message2.put("name", "Avro");
		message2.put("favorite_number", 2);
		message2.put("favorite_color", "Green");
		GenericRecord message3 = new GenericData.Record(schema);
		message3.put("name", "Example");
		message3.put("favorite_number", 3);
		message3.put("favorite_color", "Blue");
		inputData.add(message1);
		inputData.add(message2);
		inputData.add(message3);
		Generic.serialize(inputData, schemaFile, outputFile);
		List<GenericRecord> outputData = Generic.deserialize(outputFile);
		for (int i = 0; i < inputData.size(); i++) {
			assertSame(inputData.get(i), outputData.get(i));
		}
	}

	public void assertSame(GenericRecord record1, GenericRecord record2) {
		assertEquals(record1.get("name"),
				((Utf8) record2.get("name")).toString());
		assertEquals(record1.get("favorite_number"),
				((Integer) record2.get("favorite_number")).intValue());
		assertEquals(record1.get("favorite_color"),
				((Utf8) record2.get("favorite_color")).toString());
	}
}
