package avro.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import avro.example.schema.User;

/**
 * Using Avro generated code
 * 
 * @author pratyush
 * 
 */
public class GeneratedCode {
	/**
	 * User.java is generated file by avro plugin. This method binary serializes
	 * using generated User class and embeds schema
	 */
	public static void binarySerializeWithSchema(List<User> data,
			File schemaFile, File outputFile) throws IOException {
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

	/**
	 * This method binary serializes using generated User class and doesnot
	 * embed schema
	 * 
	 */
	public static void binarySerializeWithoutSchema(List<User> data,
			File outputFile) throws IOException {
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(
				User.class);
		Encoder e = EncoderFactory.get().binaryEncoder(
				new FileOutputStream(outputFile), null);
		for (User gr : data) {
			userDatumWriter.write(gr, e);
		}
		e.flush();
	}

	/**
	 * Schema is read from inside the file by dataFileReader
	 */
	public static List<User> deserializeWithoutSchema(File inputFile)
			throws IOException {
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

	/**
	 * Schema is explicitly given
	 */
	public static List<User> deserializeWithSchema(File inputFile)
			throws IOException {
		SpecificDatumReader<User> datumReader = new SpecificDatumReader<User>(
				User.class);
		Decoder dec = DecoderFactory.get().binaryDecoder(
				new FileInputStream(inputFile), null);
		User message = null;
		List<User> records = new ArrayList<User>();
		try {
			while ((message = datumReader.read(null, dec)) != null) {
				records.add(message);
			}
		} catch (Exception e) {
			// e.printStackTrace();
		}
		return records;
	}
}
