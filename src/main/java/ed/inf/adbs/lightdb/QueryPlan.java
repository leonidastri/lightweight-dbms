package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.*;

public class QueryPlan {
    // fromItem as string
    private String fromItemStr;
    // joins as strings
    private List<String> joinsStr;
    // map with aliases as keys and tables as values
    private final LinkedHashMap<String,String> aliases = new LinkedHashMap<>();
    // map with aliases as keys and the expressions in which they are included as values
    private final LinkedHashMap<String, Expression> splitWhile = new LinkedHashMap<>();
    // select operators
    private final LinkedHashMap<String, SelectOperator> selectOps = new LinkedHashMap<>();
    // scan operators
    private final LinkedHashMap<String,ScanOperator> scanOps = new LinkedHashMap<>();

    /**
     * Create query plan and return the root operator to Query Parser
     * to dumb the result
     *
     * @param select Select statement found from input file
     * @return root operator
     * @throws FileNotFoundException (for file not found)
     * @throws JSQLParserException (for JSQLParser)
     */
    public Operator createQueryPlan(Select select) throws FileNotFoundException, JSQLParserException {

        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        FromItem fromItem = plainSelect.getFromItem();
        List<Join> joins = plainSelect.getJoins();
        Expression where = plainSelect.getWhere();
        List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
        Distinct distinct = plainSelect.getDistinct();

        // Get aliases if exist
        getAliases(fromItem,joins);

        // Get fromItem as string
        if (LightDB.database.getTable(fromItem.toString())!=null)
            fromItemStr = fromItem.toString();
        else
            fromItemStr = fromItem.toString().split(" ")[1];

        joinsStr = new ArrayList<>();
        getJoinsStrings(joins, joinsStr);

        // Split WHILE clause and create a map for tables to which expressions they participate
        splitWhile();
        if (where != null)
            ConjuctVisitor.createTableExpression(where, splitWhile);
        //System.out.println(splitWhile);

        // Create scan operators
        createScanOps();
        Operator operator = scanOps.get(fromItemStr);

        // Create select operators
        createSelectOps();
        if (selectOps.containsKey(fromItemStr)) {
            operator = selectOps.get(fromItemStr);
        }

        // create join operators (left join tree) and return the root join operator
        if (joinsStr.size()!=0)
            operator = createJoinOp();

        // create project operator
        operator = new ProjectOperator(selectItems, operator,aliases);

        boolean sorted = false;
        if (orderByElements!=null) {
            // add sort operator in query plan
            SortOperator sortOperator = new SortOperator(operator,orderByElements);
            sortOperator.sort(aliases);
            operator = sortOperator;
            sorted = true;
        }

        if (distinct!=null) {
            // if it is not sorted, sort it
            if (!sorted) {
                SortOperator sortOperator = new SortOperator(operator, null);
                sortOperator.sort(aliases);
                operator = sortOperator;
            }
            // add DuplicateEliminator operator
            operator = new DuplicateEliminationOperator(operator);
        }

        operator.setSelectItemList(selectItems);
        operator.setAliases(aliases);
        // return root operator to dumb output by calling getNextTuple
        return operator;
    }

    /**
     * Get join tables as strings
     *
     * @param joins joins as Join items
     * @param joinsStr joins as Strings
     */
    private static void getJoinsStrings(List<Join> joins,
                                        List<String> joinsStr) {
        if (joins!=null) {
            for (Join join : joins) {
                if (LightDB.database.getTable(join.toString()) != null)
                    joinsStr.add(join.toString());
                else
                    joinsStr.add(join.toString().split(" ")[1]);
            }
        }
    }

    /**
     * Initialize splitWhile map variable to store tables (or aliases) and
     * the expressions in which they are included
     */
    private void splitWhile() {
        splitWhile.put(fromItemStr, null);
        if (joinsStr != null) {
            // put pairs of fromItem and each join
            for (String s : joinsStr) {
                splitWhile.put(fromItemStr + s, null);
                splitWhile.put(s + fromItemStr, null);
            }
            // put all pairs of joins
            for (String s1 : joinsStr) {
                splitWhile.put(s1,null);
                for (String s2 : joinsStr) {
                    if (!s1.equals(s2))
                        splitWhile.put(s1+s2,null);
                }
            }
        }
    }

    /**
     * Create a Map of aliases with key the alias and value the table
     *
     * @param fromItem from item
     * @param joins joins
     */
    private void getAliases(FromItem fromItem, List<Join> joins) {
        // Check if there is alias in fromItem
        if (fromItem.getAlias()!=null) {
            String alias = fromItem.getAlias().getName();
            String table = fromItem.toString().split(" ")[0];
            //System.out.println(alias + " " + table);
            aliases.put(alias,table);
        }

        // if there are joins, check if there are aliases in joins
        if (joins!=null) {
            for (Join join : joins) {
                if (join.getRightItem().getAlias() != null) {
                    String alias = join.getRightItem().getAlias().getName();
                    String table = join.getRightItem().toString().split(" ")[0];
                    aliases.put(alias, table);
                }
            }
        }
    }

    /**
     * Create a scan operator for every table in FROM clause
     *
     * @throws FileNotFoundException (file not found to read)
     */
    private void createScanOps() throws FileNotFoundException {

        // add scan operator for fromItem
        ScanOperator scanOperator = new ScanOperator(fromItemStr,aliases);
        scanOps.put(fromItemStr,scanOperator);
        if (joinsStr != null) {
            // add scan operator for joins
            for (String s : joinsStr) {
                scanOperator = new ScanOperator(s, aliases);
                scanOps.put(s, scanOperator);
            }
        }
    }

    /**
     * Create a select operator for tables in FROM clause which are referenced in WHERE clause
     */
    private void createSelectOps() {

        // add select operator for fromItem
        // if there is an expression in WHERE clause for this table
        if (splitWhile.containsKey(fromItemStr) && splitWhile.get(fromItemStr) != null) {
            SelectOperator selectOp = new SelectOperator(splitWhile.get(fromItemStr), scanOps.get(fromItemStr), aliases);
            selectOps.put(fromItemStr,selectOp);
        }

        if (joinsStr != null) {
            // add select operator for joins
            for (String join : joinsStr) {
                // if there is an expression in WHERE clause for this table
                if (splitWhile.containsKey(join) && splitWhile.get(join) != null) {
                    SelectOperator selectOp = new SelectOperator(splitWhile.get(join), scanOps.get(join), aliases);
                    selectOps.put(join, selectOp);
                }
            }
        }
    }

    /**
     *  Create join operators and return the top join operator
     *
     * @return the top join
     */
    private JoinOperator createJoinOp() throws JSQLParserException {
        ArrayList<String> prevTables = new ArrayList<>();
        Operator operator1;
        Operator operator2;
        JoinOperator joinOperator = null;

        // if there exists a select operator use this
        if (selectOps.containsKey(fromItemStr))
            operator1 = selectOps.get(fromItemStr);
        // else use scan operator
        else
            operator1 = scanOps.get(fromItemStr);

        String prev = fromItemStr;
        prevTables.add(prev);
        // create left join tree (start with fromItem and first joinItem and
        // then join them with the next joinItem and so on)
        for (String join : joinsStr) {
            // if there exists a select operator use this
            if (selectOps.containsKey(join))
                operator2 = selectOps.get(join);
            // else use scan operator
            else
                operator2 = scanOps.get(join);
            String whereExpr = null;
            // find if there is a join with one of the already joined tables
            for (int i = 0; i < prevTables.size(); i++) {
                String prevTable = prevTables.get(i);
                Expression addExpr = null;
                if (splitWhile.get(prevTable+join) != null)
                    addExpr = splitWhile.get(prevTable + join);
                else if (splitWhile.get(join+prevTable) != null)
                    addExpr = splitWhile.get(join + prevTable);

                if (addExpr!=null)
                    if (whereExpr == null)
                        whereExpr = addExpr.toString();
                    else
                        whereExpr += (" AND " + addExpr.toString());
            }
            Expression where = null;
            if (whereExpr!=null)
                where = CCJSqlParserUtil.parseCondExpression(whereExpr);

            //System.out.println(where);
            joinOperator = new JoinOperator(where, operator1, operator2, aliases);
            operator1 = joinOperator;
            prevTables.add(join);
        }
        return joinOperator;
    }
}
