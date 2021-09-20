package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Stack;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * Visitor class to evaluate an Expression value (True or False),
 * for SelectOperator. It can be used to evaluate if tuple should be
 * selected or not
 */
public class Visitor {

    /**
     * Evaluate expression for tuple in SelectOperator
     *
     * @param expr Expression to evaluate
     * @param tuple Tuple examined
     * @param aliases Map of aliases to help in evaluation
     * @return (True or False)
     */
    static boolean evaluate(Expression expr, Tuple tuple, LinkedHashMap<String,String> aliases) {
        final Stack<Object> stack = new Stack<>();
        ExpressionDeParser deparser = new ExpressionDeParser() {
            @Override
            public void visit(AndExpression andExpression) {
                super.visit(andExpression);

                // Pop right and left elements
                boolean b1 = (Boolean) stack.pop();
                boolean b2 = (Boolean) stack.pop();

                // Push result of minorThan
                stack.push(b1 && b2);
            }

            @Override
            public void visit(EqualsTo equalsTo) {
                super.visit(equalsTo);

                // Pop right and left elements
                long num2 = (Long) stack.pop();
                long num1 = (Long) stack.pop();

                // Push result of equalsTo
                stack.push(num1 == num2);
            }

            @Override
            public void visit(NotEqualsTo notEqualsTo) {
                super.visit(notEqualsTo);

                // Pop right and left elements
                long num2 = (Long) stack.pop();
                long num1 = (Long) stack.pop();

                // Push result of notEqualsTo
                stack.push(num1 != num2);
            }

            @Override
            public void visit(GreaterThan greaterThan) {
                super.visit(greaterThan);

                // Pop right and left elements
                long num2 = (Long) stack.pop();
                long num1 = (Long) stack.pop();

                // Push result of greaterThan
                stack.push(num1 > num2);
            }

            @Override
            public void visit(MinorThan minorThan) {
                super.visit(minorThan);

                // Pop right and left elements
                long num2 = (Long) stack.pop();
                long num1 = (Long) stack.pop();

                // Push result of minorThan
                stack.push(num1 < num2);
            }
            @Override
            public void visit(GreaterThanEquals greaterThanEquals) {
                super.visit(greaterThanEquals);

                // Pop right and left elements
                long num2 = (Long) stack.pop();
                long num1 = (Long) stack.pop();

                // Push result of greaterThanEquals
                stack.push(num1 >= num2);
            }
            @Override
            public void visit(MinorThanEquals minorThanEquals) {
                super.visit(minorThanEquals);

                // Pop right and left elements
                long num2 = (Long) stack.pop();
                long num1 = (Long) stack.pop();

                // Push result of minorThanEquals
                stack.push(num1 <= num2);
            }

            @Override
            public void visit(LongValue longValue) {
                super.visit(longValue);

                // Push long value
                stack.push(longValue.getValue());
            }

            @Override
            public void visit(Column column) {
                super.visit(column);
                // Get table and column name from column
                String tableName = column.getTable().getName();
                String columnName = column.getColumnName();
                // Get table/tables and their column values stored in tuple
                LinkedHashMap<String,ArrayList<String>> values = tuple.getValues();

                String tupleTable = tableName;
                // if we have an alias we need to get real table
                if (LightDB.database.getTable(tableName)==null) {
                    tableName = aliases.get(tableName);
                }
                // Get position number of specific column name of specific table from stored db
                Integer columnPos = LightDB.database.getTable(tableName).get(columnName);
                // Push specific value of tuple (specific table and specific column value)
                stack.push(Long.parseLong(values.get(tupleTable).get(columnPos),10));
            }
        };

        StringBuilder b = new StringBuilder();
        deparser.setBuffer(b);
        expr.accept(deparser);

        // Return value of expression (True or False)
        return (boolean) stack.pop();
    }
}