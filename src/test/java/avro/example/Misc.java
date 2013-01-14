package avro.example;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

public class Misc {
	public static File getNonExistingTemporaryDirectoryName()
			throws IOException {
		File tempFile = File.createTempFile("avro-mr-temp", "dir");
		if (!tempFile.delete()) {
			fail("Could not delete temp file. Cannot guarantee non-existing temp dir name.");
		}

		return tempFile;
	}
}
