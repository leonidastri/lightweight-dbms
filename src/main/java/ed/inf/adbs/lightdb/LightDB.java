package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedHashMap;
import java.util.Scanner;

/**
 * LightDB class
 *
 * -Contains main method of the project-
 */
public class LightDB {
	// Database object to store path of files and schema of database
	public static Database database = Database.getInstance();
	// var to access data directory
	private static final String DATA = "data";
	// var to access schema file
	private static final String SCHEMA = "schema.txt";

	/**
	 * Main function of my project
	 *
	 * @param args Arguments of LightDB program
	 * @throws FileNotFoundException (cannot read files)
	 */
	public static void main(String[] args) throws FileNotFoundException {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		// Arguments given to LightDB program
		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		// Store information about the database in instance of Database (Singleton pattern)
		populateDatabase(LightDB.database,databaseDir);
		QueryParser parser = new QueryParser();
		// Parse query
		parser.parseQuery(inputFile,outputFile);
	}

	/**
	 * Scan db dir to get file paths and schema and store them to Database object (Singleton pattern)
	 *
	 * @param database Database object to store file paths and
	 * @param databaseDir Directory of database given as input
	 * @throws FileNotFoundException (cannot read files)
	 */
	public static void populateDatabase(Database database, String databaseDir) throws FileNotFoundException {
		int i;
		File dir = new File(databaseDir);
		File[] files = dir.listFiles();

		// files and dirs in databaseDir
		if (files != null) {
			for(i = 0; i < files.length; i++) {
				// if it is file
				if (files[i].isFile()) {
					// if it is schema.txt
					if (files[i].getName().equals(SCHEMA)) {
						Scanner scanner = new Scanner(files[i]);
						while (scanner.hasNextLine()) {
							String line = scanner.nextLine();
							String[] lineSplit = line.split(" ");
							LinkedHashMap<String,Integer> columns = new LinkedHashMap<>();
							for(int k = 1; k < lineSplit.length; k++) {
								columns.put(lineSplit[k],k-1);
							}
							database.addTable(lineSplit[0],columns);
						}
					}
				// if its directory
				} else if (files[i].isDirectory()) {
					// if it is data directory
					if (files[i].getName().equals(DATA)) {
						File dataDir = new File(files[i].getPath());
						File[] dataFiles = dataDir.listFiles();
						// for all files in data dir store name and path
						if (dataFiles!=null) {
							for (File dataFile : dataFiles)
								if (dataFile.isFile())
									database.addLocation(dataFile.getName().substring(0,
											         dataFile.getName().lastIndexOf('.')),
													 dataFile.getPath());
						}
					}
				}
			}
		}
	}
}
