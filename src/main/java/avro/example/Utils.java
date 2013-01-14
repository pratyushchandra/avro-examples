package avro.example;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

public class Utils {
	public static Schema getSchema(File schemaFile) throws IOException {
		return new Parser().parse(schemaFile);
	}
}
