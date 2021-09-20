package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JoinOperator class
 *
 * -create join operator with two other operators as children-
 */
public class JoinOperator extends Operator {
    // Where clause containing tables from the two joined operators
    private final Expression where;
    // tuple var for managing tuples left child operator
    private Tuple tuple1;
    // tuple var for managing tuples right child operator
    private Tuple tuple2;
    // left child operator of join operator
    private final Operator leftChild;
    // right child operator of join operator
    private final Operator rightChild;
    // map with aliases as keys and real tables as values
    private final LinkedHashMap<String,String> aliases;

    /**
     * Constructor
     *
     * @param whereExpression where clause containing tables from the two joined operators
     * @param o1 left child operator
     * @param o2 right child operator
     * @param al map with aliases as keys and real tables as values
     */
    public JoinOperator(Expression whereExpression, Operator o1, Operator o2,
                        LinkedHashMap<String,String> al) {
        where = whereExpression;
        tuple1 = null;
        tuple2 = null;
        leftChild = o1;
        rightChild = o2;
        aliases = al;
    }

    /**
     * get next tuple
     *
     * @return next tuple
     * @throws JSQLParserException (JSQLParser exception)
     * @throws FileNotFoundException (file not found to read)
     */
    public Tuple getNextTuple() throws JSQLParserException, FileNotFoundException {

        // if left-child tuple is null get next tuple
        if (tuple1 == null)
            tuple1 = leftChild.getNextTuple();

        // while left-child tuple is not null
        while (tuple1 != null) {
            // get next tuple of right child
            tuple2 = rightChild.getNextTuple();

            // if next tuple of right child is null
            if (tuple2 == null) {
                // reset right child operator
                this.reset();
                // get next tuple of both left and right operators
                tuple1 = leftChild.getNextTuple();
                tuple2 = rightChild.getNextTuple();
            }
            // if tuple of left child is null return null
            if (tuple1 == null)
                return null;

            // join tuples to create new joined tuple
            Tuple joinTuple = new Tuple(tuple1.getValues());
            LinkedHashMap<String,ArrayList<String>> values2 = tuple2.getValues();
            for(Map.Entry entry : values2.entrySet())
                joinTuple.getValues().put((String) entry.getKey(), (ArrayList<String>) entry.getValue());

            // if there is no where clause for joined tuple return tuple
            if (where == null)
                return joinTuple;
            // if there is where clause check if it is satisfied
            if (Visitor.evaluate(where, joinTuple, aliases))
                return joinTuple;
        }
        return null;
    }

    // reset right child to help with implementation of the nested loop join algorithm
    public void reset() throws FileNotFoundException {
        rightChild.reset();
    }
}