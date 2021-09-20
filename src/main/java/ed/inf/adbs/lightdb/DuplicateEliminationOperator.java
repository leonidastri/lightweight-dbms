package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * DuplicateEliminationOperator class
 *
 * -eliminate duplicates-
 */
public class DuplicateEliminationOperator extends Operator {
    // child operator
    private final Operator child;
    // previous tuple
    private Tuple prevTuple;

    /**
     * Constructor
     *
     * @param operator child operator
     */
    public DuplicateEliminationOperator(Operator operator) {
        this.child = operator;
        prevTuple = null;
    }

    /**
     * Get next tuple
     *
     * @return next tuple
     * @throws JSQLParserException (JSQLParser exception)
     * @throws FileNotFoundException (file not found to read)
     */
    public Tuple getNextTuple() throws JSQLParserException, FileNotFoundException {

        Tuple tuple = child.getNextTuple();

        // while child operator has next tuple
        while (tuple!=null) {

            LinkedHashMap<String, ArrayList<String>> tupleValues2 = null;
            // if previous tuple is not null get its column values
            if (prevTuple != null)
                tupleValues2 = prevTuple.getValues();

            // get tuple values of current tuple
            LinkedHashMap<String, ArrayList<String>> tupleValues1 = tuple.getValues();

            if (tupleValues2 != null) {
                // for every table in tuple
                for (String key : tupleValues1.keySet()) {
                    // compare columns of current tuple with previous tuple and if they are not
                    // equal return current tuple
                    if (!tupleValues1.get(key).equals(tupleValues2.get(key))) {
                        // set previous tuple as current tuple for next call
                        prevTuple = tuple;
                        return tuple;
                    }
                }
            } else {
                prevTuple = tuple;
                return tuple;
            }
            prevTuple = tuple;
            tuple = child.getNextTuple();
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
