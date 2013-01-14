package avro.example;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import avro.example.schema.User;

public class GeneratedCode {
	/*
	 * User.java is generated file by avro plugin This method serializes using
	 * generated User class
	 */
	public static void serialize(List<User> data, File schemaFile,
			File outputFile) throws IOException {
		Schema schema = Utils.getSchema(schemaFile);
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(
				User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(
				userDatumWriter);
		dataFileWriter.create(schema, outputFile);
		for (User gr : data) {
			dataFileWriter.append(gr);
		}
		dataFileWriter.close();
	}

	/*
	 * Schema is read from inside the file by dataFileReader
	 */
	public static List<User> deserialize(File inputFile) throws IOException {
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(
				User.class);
		DataFileReader<User> dataFileReader = new DataFileReader<User>(
				inputFile, userDatumReader);
		List<User> records = new ArrayList<User>();
		while (dataFileReader.hasNext()) {
			records.add(dataFileReader.next());
		}
		return records;
	}
}
