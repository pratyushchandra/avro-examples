package avro.example;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.junit.BeforeClass;
import org.junit.Test;

import avro.example.schema.User;

public class TestGeneratedCode {
	private static File schemaFile;

	@BeforeClass
	public static void before() throws IOException {
		schemaFile = new File("src/main/resources/avro/users.avsc");
	}

	@Test
	public void test1() throws IOException {
		File outputFile = Misc.getNonExistingTemporaryDirectoryName();
		List<User> inputData = new ArrayList<User>();
		User user1 = new User();
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		user1.setFavoriteColor("yellow");

		// Alternate constructor
		User user2 = new User("Ben", 7, "red");

		// Construct via builder
		User user3 = User.newBuilder().setName("Charlie")
				.setFavoriteColor("blue").setFavoriteNumber(33).build();

		inputData.add(user1);
		inputData.add(user2);
		inputData.add(user3);
		GeneratedCode.serialize(inputData, schemaFile, outputFile);
		List<User> outputData = GeneratedCode.deserialize(outputFile);
		for (int i = 0; i < inputData.size(); i++) {
			assertSame(inputData.get(i), outputData.get(i));
		}
	}

	public void assertSame(User record1, User record2) {
		assertEquals(record1.getName(), ((Utf8) record2.getName()).toString());
		assertEquals(record1.getFavoriteNumber(),
				(Integer) record2.getFavoriteNumber());
		assertEquals(record1.getFavoriteColor(),
				((Utf8) record2.getFavoriteColor()).toString());
	}
}
