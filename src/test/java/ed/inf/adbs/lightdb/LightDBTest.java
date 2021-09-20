package ed.inf.adbs.lightdb;

import static org.junit.Assert.fail;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Scanner;

/**
 * Unit test for simple LightDB.
 */
public class LightDBTest {

	// var to access data directory
	private static final String DATA = "data";
	// var to access schema file
	private static final String SCHEMA = "schema.txt";

	/**
	 * db read test for query1 to check if the input files are stored
	 *
	 * @throws IOException (input file operation)
	 */
	@Test
	public void dbTest() throws IOException {
		String[] args = {
				"samples/db",
				"./samples/input/query1.sql",
				"./samples/output/query1.csv"
		};

		LightDB.main(args);
		File dir = new File(args[0]);
		File[] files = dir.listFiles();

		int i;
		if (files!=null) {
			for (i = 0; i < files.length; i++) {
				if (files[i].isFile()) {
					// check schema.txt file
					if (files[i].getName().equals(SCHEMA)) {
						Scanner scanner = new Scanner(files[i]);
						// check each line (table)
						while (scanner.hasNextLine()) {
							String line = scanner.nextLine();
							String[] lineSplit = line.split(" ");
							LinkedHashMap<String, Integer> columns = LightDB.database.getTable(lineSplit[0]);
							// check columns for each line
							for (int k = 1; k < lineSplit.length; k++) {
								// if a column stored is wrong
								if (k - 1 != columns.get(lineSplit[k]))
									fail();
							}
						}
					}
				// check if db/data dir was read correctly
				} else if (files[i].isDirectory()) {
					if (files[i].getName().equals(DATA)) {
						File dataDir = new File(files[i].getPath());
						File[] dataFiles = dataDir.listFiles();
						if (dataFiles != null) {
							for (File dataFile : dataFiles)
								if (dataFile.isFile()) {
									String location = LightDB.database.getLocation(
											dataFile.getName().substring(0, dataFile.getName().lastIndexOf('.')));
									// if location stored is wrong
									if (!location.equals(dataFile.getPath()))
										fail();
								}
						}
					}
				}
			}
		}
	}
}
