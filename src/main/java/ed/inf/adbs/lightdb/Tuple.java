package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Tuple Class to process tuples
 */
public class Tuple {
    /**
     * Map to store tables and column values of tuple (used
     * map to keep all tables and columns in case of joins)
     */
    private LinkedHashMap<String,ArrayList<String>> columnValues;

    /**
     * Constructor (Initialize private variable)
     */
    public Tuple () {
        columnValues = new LinkedHashMap<>();
    }

    /**
     * Constructor (Initialize private variable with table and column values)
     *
     * @param vls (values of columns) for all tables
     */
    public Tuple (LinkedHashMap<String,ArrayList<String>> vls) {
        columnValues = new LinkedHashMap<>(vls);
    }

    /**
     * Add table and column values in tuple
     *
     * @param table specific table
     * @param vls values of columns for specific table
     */
    public void setValues(String table, ArrayList<String> vls) {
        columnValues.put(table,vls);
    }

    /**
     * Get table or tables and their column values stored in tuple
     *
     * @return table/tables and their column values stored in tuple
     */
    public LinkedHashMap<String,ArrayList<String>> getValues() {
        return columnValues;
    }

    /**
     * Projection of tuple (Store only projected values)
     *
     * @param projection (Just a string with no use)
     * @param vls (All projected values)
     */
    public void projectValues(String projection, ArrayList<String> vls) {
        columnValues = new LinkedHashMap<>();
        columnValues.put(projection,vls);
    }
}