package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * ProjectOperator class
 *
 * -create projected tuple-
 */
public class ProjectOperator extends Operator {
    // include all table columns in the projected tuple if true
    private final boolean includeAll;
    // tables and their columns to be projected
    private LinkedHashMap<String,ArrayList<Integer>> project;
    // Child operator
    private final Operator child;

    /**
     * Constructor
     *
     * @param items select items
     * @param op child operator
     * @param aliases map with aliases as keys and real tables as values
     */
    public ProjectOperator(List<SelectItem> items, Operator op, LinkedHashMap<String,String> aliases) {

        // if we have select * include all columns of all tables in projected tuple
        if (items.get(0).toString().equals("*"))
            includeAll = true;
        // else, include only specified columns of specified tables
        else {
            includeAll = false;
            project = new LinkedHashMap<>();
            // for every select item
            for (SelectItem item : items) {
                String column = item.toString();
                String[] parts = column.split(Pattern.quote("."));

                String tableName = parts[0];
                String tupleTable = tableName;
                // if we have an alias we need to get real table
                if (LightDB.database.getTable(tableName) == null)
                    tableName = aliases.get(tableName);

                // Get position number of specific column name of specific table from stored db
                Integer columnPos = LightDB.database.getTable(tableName).get(parts[1]);
                // add tables/aliases and the column positions to project tuples
                if (!project.containsKey(tupleTable)) {
                    project.put(tupleTable,new ArrayList<>());
                }
                project.get(tupleTable).add(columnPos);
            }
        }

        child = op;
    }

    /**
     * Get next tuple
     *
     * @return return projected tuple
     * @throws JSQLParserException (JSQLParser exception for using it)
     * @throws FileNotFoundException (file not found to read)
     */
    public Tuple getNextTuple() throws JSQLParserException, FileNotFoundException {

        // get tuple from child
        Tuple tuple = child.getNextTuple();

        // if it is not null
        if(tuple != null) {
            // if all columns of tables of tuple need to be projected
            if (includeAll)
                return tuple;
            // else project only specified columns of specified tables/aliases
            LinkedHashMap<String,ArrayList<String>> tupleValues = tuple.getValues();
            //System.out.println(tupleValues);
            // for every table in tuple
            for (String key : tupleValues.keySet()) {
                ArrayList<String> tableValues = tupleValues.get(key);
                ArrayList<String> projectValues = new ArrayList<>();
                // for every value of specific table in tuple
                for (int i = 0; i < tableValues.size(); i++) {
                    // if table is not to be projected
                    if (!project.containsKey(key))
                        projectValues.add(null);
                    // if table is to be projected
                    else {
                        // if value not to be projected put null
                        if (!project.get(key).contains(i))
                            projectValues.add(null);
                        // else add value to projection
                        else
                            projectValues.add(tableValues.get(i));
                    }
                }
                tuple.setValues(key,projectValues);
            }
        }

        return tuple;
    }

    /**
     * Reset operator (It is unnecessary for my implementation - keep it for future use)
     *
     * @throws FileNotFoundException (file not found to read)
     */
    public void reset() throws FileNotFoundException {
        child.reset();
    }
}
