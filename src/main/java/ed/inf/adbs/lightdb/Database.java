package ed.inf.adbs.lightdb;

import java.util.LinkedHashMap;

/**
 * Database class to store filenames and their location paths
 * and the database schema (table names and their column names)
 */
public class Database {
    // Database object (global entity)
    private static final Database database = new Database();
    // Map for database files and their location paths
    private final LinkedHashMap<String, String> locations = new LinkedHashMap<>();
    // Map for schema table names and map of their column names and columns positions
    private final LinkedHashMap<String, LinkedHashMap<String,Integer>> schema = new LinkedHashMap<>();

    /**
     * Private constructor so class cannot be instantiated
     */
    private Database() {}

    /**
     * @return instance of Database
     */
    public static Database getInstance() {
        return database;
    }

    /**
     * Store filename and its location path
     *
     * @param file filename
     * @param location location path
     */
    public void addLocation(String file, String location) {
        locations.put(file, location);
    }

    /**
     * Get location of specific filename
     *
     * @param file filename
     * @return location path of specific filename
     */
    public String getLocation(String file) {
        return locations.get(file);
    }

    /**
     * Store column names of specific table
     *
     * @param table name of table
     * @param columns columns of specific table
     */
    public void addTable(String table, LinkedHashMap<String,Integer> columns) {
        schema.put(table, columns);
    }

    /**
     * Get column names of specific table
     *
     * @param table name of table
     * @return columns of specific table
     */
    public LinkedHashMap<String,Integer> getTable(String table) {
        return schema.get(table);
    }
}