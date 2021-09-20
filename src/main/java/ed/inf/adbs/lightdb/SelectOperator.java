package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import java.io.FileNotFoundException;
import java.util.LinkedHashMap;

/**
 * SelectOperator class
 *
 * -check where clause satisfaction-
 */
public class SelectOperator extends Operator {
    // Where clause for table we want to have a select operator
    private final Expression where;
    // associated operator (scan operator)
    private final Operator child;
    // map with aliases as keys and real tables as values
    private final LinkedHashMap<String,String> aliases;

    /**
     * Constructor
     *
     * @param whereExpression where clause expression of table (scan Operator table) which must be satisfied by tuples
     * @param scanOp scan operator associated with select operator
     * @param al aliases map
     */
    public SelectOperator(Expression whereExpression, ScanOperator scanOp, LinkedHashMap<String,String> al) {
        where = whereExpression;
        child = scanOp;
        aliases = al;
    }

    /**
     * Get next tuple from child (associated scan operator) which satisfies the where clause expression
     *
     * @return next tuple
     */
    public Tuple getNextTuple() throws FileNotFoundException, JSQLParserException {

        Tuple tuple = child.getNextTuple();
        // get next tuple until the where clause expression is satisfied
        while (tuple != null && !Visitor.evaluate(where, tuple, aliases)) {
            tuple = child.getNextTuple();
        }
        return tuple;
    }

    /**
     * Reset operator by calling associated scan operator's reset method
     *
     * @throws FileNotFoundException (File not found)
     */
    public void reset() throws FileNotFoundException {
        child.reset();
    }
}

