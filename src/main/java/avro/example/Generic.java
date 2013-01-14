package avro.example;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class Generic {

	/*
	 * DataFileWriter creates a outputFile containing schema in json and binary
	 * serializes all the data
	 */
	public static void serialize(List<GenericRecord> data, File schemaFile,
			File outputFile) throws IOException {
		Schema schema = Utils.getSchema(schemaFile);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
				schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				datumWriter);
		dataFileWriter.create(schema, outputFile);
		for (GenericRecord gr : data) {
			dataFileWriter.append(gr);
		}
		dataFileWriter.close();
	}

	/*
	 * Schema is read from inside the file by dataFileReader
	 */
	public static List<GenericRecord> deserialize(File inputFile)
			throws IOException {
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				inputFile, datumReader);
		List<GenericRecord> records = new ArrayList<GenericRecord>();
		while (dataFileReader.hasNext()) {
			records.add(dataFileReader.next());
		}
		return records;
	}

}
