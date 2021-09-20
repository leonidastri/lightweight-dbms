package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * SortOperator class
 *
 * -sort tuples-
 */
public class SortOperator extends Operator {
    // all tuples of output
    private final List<Tuple> tuples;
    // all order-by items to sort output tuples
    private final List<OrderByElement> orderByElements;
    // child operator
    private final Operator child;

    /**
     * Constructor
     *
     * @param operator child operator
     * @param orderByElements order-by items for sorting
     */
    public SortOperator(Operator operator, List<OrderByElement> orderByElements) {
        tuples = new ArrayList<>();
        this.orderByElements = orderByElements;
        child = operator;
    }

    /**
     * Get all tuples from child operator and sort them using order-by elements (or from first column
     * to last if we want to eliminate duplicates.. this is for DuplicateEliminationOperator)
     *
     * @param aliases map with aliases as keys and real tables as values
     * @throws FileNotFoundException (file not found to read)
     * @throws JSQLParserException (JSQLParser exception)
     */
    public void sort(LinkedHashMap<String,String> aliases) throws FileNotFoundException, JSQLParserException {

        Tuple tuple;
        // get all tuples from child operator
        while((tuple = child.getNextTuple()) != null)
            tuples.add(tuple);

        Comparator<Tuple> comparator_tuples = (tuple1, tuple2) -> {
            // for every order-by element
            if (orderByElements!=null) {
                for (OrderByElement el : orderByElements) {
                    Column column = (Column) el.getExpression();
                    // get tableName and columnName for comparing the two tuples
                    String tableName = column.getTable().getName();
                    String columnName = column.getColumnName();

                    String tupleTable = tableName;
                    if (LightDB.database.getTable(tableName) == null)
                        tableName = aliases.get(tableName);

                    // Get specified table columns for each of the two tuples
                    ArrayList<String> values1 = tuple1.getValues().get(tupleTable);
                    ArrayList<String> values2 = tuple2.getValues().get(tupleTable);
                    // get position of column to compare
                    Integer columnPos = LightDB.database.getTable(tableName).get(columnName);

                    String val1 = values1.get(columnPos);
                    String val2 = values2.get(columnPos);
                    //compare their values as integers not strings
                    if (Integer.parseInt(val1) > Integer.parseInt(val2))
                        return 1;
                    else if (Integer.parseInt(val1) < Integer.parseInt(val2))
                        return -1;
                }
            }

            // If there are no order-by elements or values cannot be sorted by order-by elements
            // because of equality in these values

            // get tables as inserted
            for (String key : tuple1.getValues().keySet()) {
                LinkedHashMap<String, ArrayList<String>> values2 = tuple2.getValues();
                // for every column compare their values until they can be sorted
                for(int i = 0; i < tuple1.getValues().get(key).size(); i++) {
                    String val1 = tuple1.getValues().get(key).get(i);
                    String val2 = values2.get(key).get(i);
                    // check only projected values
                    if (val1 == null)
                        continue;
                    //compare their values as integers not strings
                    if (Integer.parseInt(val1) > Integer.parseInt(val2))
                        return 1;
                    else if (Integer.parseInt(val1) < Integer.parseInt(val2))
                        return -1;
                }
            }
            return 0;
        };

        tuples.sort(comparator_tuples);
    }

    /**
     * Get next tuple
     *
     * @return next tuple
     */
    public Tuple getNextTuple() {
        if (tuples.size() > 0) {
            Tuple tuple = tuples.get(0);
            tuples.remove(0);
            return tuple;
        }
        return null;
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
