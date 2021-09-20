package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Scanner;

/**
 * ScanOperator class
 *
 * -scan table-
 */
public class ScanOperator extends Operator {
    // table in which we perform scan
    private final String table;
    private final String alias;
    private Scanner scanner;

    /**
     * Constructor (Set table and file to scan)
     *
     * @param fromItem table in which the scan will happen
     * @throws FileNotFoundException (file not found)
     */
    public ScanOperator(String fromItem, LinkedHashMap<String,String> aliases) throws FileNotFoundException {
        alias = fromItem;

        String tempTable = fromItem;
        // if fromItem is an alias we want to get real table
        if (LightDB.database.getTable(fromItem)==null)
            tempTable = aliases.get(fromItem);
        table = tempTable;

        scanner = new Scanner(new File(LightDB.database.getLocation(table)));
    }

    /**
     * Get next tuple
     *
     * @return next tuple
     */
    public Tuple getNextTuple() {

        // check if next line exist
        if (scanner.hasNextLine()) {
            // get next line and split it to tuple values
            ArrayList<String> columns = new ArrayList<>(Arrays.asList(scanner.nextLine().split(",")));
            // create tuple
            Tuple tuple = new Tuple();
            tuple.setValues(alias,columns);
            return tuple;
        }
        return null;
    }

    /**
     * Reset operator by start reading again the file
     *
     * @throws FileNotFoundException (file not found to read)
     */
    public void reset() throws FileNotFoundException {
        scanner = new Scanner(new File(LightDB.database.getLocation(table)));
    }
}
