package ed.inf.adbs.lightdb;

import java.util.LinkedHashMap;
import java.util.Stack;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
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
 * Conjuct visitor to split WHERE expression to parts for
 * each table in FROM and for joins. For example, if the
 * qyery is SELECT * FROM T1, T2, T3 WHERE T1.A = 5 AND T1.B = T2.B AND T3.B > 5
 * we store in a hashmap the following (table,expression) pairs:
 * (T1,T1.A), (T1T2,T2.B = T2.B), (T3,T3.B)
 */
public class ConjuctVisitor {

    static void createTableExpression(Expression expr, LinkedHashMap<String,Expression> tableExpressions) {
        final Stack<String> stack = new Stack<>();
        ExpressionDeParser deparser = new ExpressionDeParser() {
            @Override
            public void visit(AndExpression andExpression) {
                super.visit(andExpression);
                // Push null because we do not have a table
                stack.push(null);
            }

            @Override
            public void visit(EqualsTo equalsTo) {
                super.visit(equalsTo);
                try {
                    // Pop right and left elements and if we have tables in expression we
                    // need to add it to the map of tables and expressions of these tables
                    addExpression(stack.pop(),stack.pop(),tableExpressions,equalsTo);
                } catch (JSQLParserException e) {
                    e.printStackTrace();
                }
                // Push null because we do not have a table
                stack.push(null);
            }

            @Override
            public void visit(NotEqualsTo notEqualsTo) {
                super.visit(notEqualsTo);
                try {
                    // Pop right and left elements and if we have tables in expression we
                    // need to add it to the map of tables and expressions of these tables
                    addExpression(stack.pop(),stack.pop(),tableExpressions,notEqualsTo);
                } catch (JSQLParserException e) {
                    e.printStackTrace();
                }
                // Push null because we do not have a table
                stack.push(null);
            }

            @Override
            public void visit(GreaterThan greaterThan) {
                super.visit(greaterThan);
                try {
                    // Pop right and left elements and if we have tables in expression we
                    // need to add it to the map of tables and expressions of these tables
                    addExpression(stack.pop(),stack.pop(),tableExpressions,greaterThan);
                } catch (JSQLParserException e) {
                    e.printStackTrace();
                }
                // Push null because we do not have a table
                stack.push(null);
            }

            @Override
            public void visit(MinorThan minorThan) {
                super.visit(minorThan);
                try {
                    // Pop right and left elements and if we have tables in expression we
                    // need to add it to the map of tables and expressions of these tables
                    addExpression(stack.pop(),stack.pop(),tableExpressions,minorThan);
                } catch (JSQLParserException e) {
                    e.printStackTrace();
                }
                // Push null because we do not have a table
                stack.push(null);
            }
            @Override
            public void visit(GreaterThanEquals greaterThanEquals) {
                super.visit(greaterThanEquals);
                try {
                    // Pop right and left elements and if we have tables in expression we
                    // need to add it to the map of tables and expressions of these tables
                    addExpression(stack.pop(),stack.pop(),tableExpressions,greaterThanEquals);
                } catch (JSQLParserException e) {
                    e.printStackTrace();
                }
                // Push null because we do not have a table
                stack.push(null);
            }
            @Override
            public void visit(MinorThanEquals minorThanEquals) {
                super.visit(minorThanEquals);
                try {
                    // Pop right and left elements and if we have tables in expression we
                    // need to add it to the map of tables and expressions of these tables
                    addExpression(stack.pop(),stack.pop(),tableExpressions,minorThanEquals);
                } catch (JSQLParserException e) {
                    e.printStackTrace();
                }
                // Push null because we do not have a table
                stack.push(null);
            }

            @Override
            public void visit(LongValue longValue) {
                super.visit(longValue);
                // Push null because we do not have a table
                stack.push(null);
            }

            @Override
            public  void visit(Column column) {
                super.visit(column);

                String tableName = column.getTable().getName();
                // Push table (it may be an alias or real table)
                stack.push(tableName);
            }
        };

        StringBuilder b = new StringBuilder();
        deparser.setBuffer(b);
        expr.accept(deparser);
    }

    /**
     * Stores tables and the expression in which they are used in WHERE clause
     *
     * @param term1 table candidate 1 (may be null if no table in expression)
     * @param term2 table candidate 2 (may be null if no table in expression)
     * @param tableExpressions map of tables to their expressions from WHERE clause
     * @param addExpr expression to add
     * @throws JSQLParserException (for JSQLParser)
     */
    private static void addExpression(String term1, String term2, LinkedHashMap<String,Expression> tableExpressions,
                                      Expression addExpr) throws JSQLParserException {
        String key = null;
        String expression = null;

        // Check if key exists in map
        if (tableExpressions.containsKey(term1 + term2))
            key = term2 + term1;
        else if (tableExpressions.containsKey(term2 + term1))
            key = term1 + term2;
        else if (tableExpressions.containsKey(term1))
            key = term1;
        else if (tableExpressions.containsKey(term2))
            key = term2;
        // if key exists (expression with table/tables(joins))
        //System.out.println(key + " " + addExpr);
        if (key != null) {
            // if there is expression for this table/tables (joins) concatenate new expression
            // with current one
            if (tableExpressions.get(key) != null)
                expression = tableExpressions.get(key).toString();
            if (expression != null)
                expression += (" AND " + addExpr.toString());
            // else create new one
            else
                expression = addExpr.toString();
            Expression newExpression = CCJSqlParserUtil.parseCondExpression(expression);
            // add expression which contains table/tables in map
            tableExpressions.put(key, newExpression);

        // if we do not have key (expression with no table/tables(joins)) add expression to every
        // table/tables(joins) key in map
        } else {
            for (String k : tableExpressions.keySet()) {
                // if there is expression for this table/tables(joins) concatenate new expression
                // with current one
                if (tableExpressions.get(k) != null)
                    expression = tableExpressions.get(k).toString();
                if (expression != null)
                    expression += (" AND " + addExpr.toString());
                // else create new one
                else
                    expression = addExpr.toString();
                Expression newExpression = CCJSqlParserUtil.parseCondExpression(expression);
                // add expression which contains table/tables in map
                tableExpressions.put(k, newExpression);
                expression = null;
            }
        }
    }
}