package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class Operator {

    private List<SelectItem> selectItemList = null;
    private LinkedHashMap<String,String> aliases;

    /**
     * set select items
     *
     * @param selectItemList list of select items
     */
    public void setSelectItemList(List<SelectItem> selectItemList) {
        this.selectItemList = selectItemList;
    }

    /**
     * set aliases
     *
     * @param aliases map with aliases as keys and real tables as values
     */
    public void setAliases(LinkedHashMap<String,String> aliases) {
        this.aliases = aliases;
    }
    /**
     * Get next tuple
     *
     * @return next tuple of the operator
     * @throws JSQLParserException (JSQLParser used in operators)
     * @throws FileNotFoundException (File not found to read)
     */
    public abstract Tuple getNextTuple() throws JSQLParserException, FileNotFoundException;

    /**
     * Reset operator
     *
     * @throws FileNotFoundException (File not found to read)
     */
    public abstract void reset() throws FileNotFoundException;

    /**
     * Write output to suitable PrintStream (call getNextTuple repeatedly)
     *
     * @param outputFile given by user
     * @throws FileNotFoundException (File not found to read)
     * @throws JSQLParserException (JSQLParser used in operators)
     */
    public void dump(String outputFile) throws FileNotFoundException, JSQLParserException {

        boolean newLine = false;
        boolean putComma;
        Tuple tuple;
        PrintStream output = new PrintStream(outputFile);
        System.out.println("Result:");

        // for every tuple
        while ((tuple = getNextTuple()) != null) {

            // if not first line value start new line
            if (newLine) {
                System.out.println();
                output.println();
            }

            // print tuple to the output file
            LinkedHashMap<String,ArrayList<String>> tupleValues = tuple.getValues();
            putComma = false;
            // if we do not have select items print all with order of tables in from clause
            if (selectItemList.get(0).toString().equals("*")) {
                for (Map.Entry entry : tupleValues.entrySet()) {
                    ArrayList<String> columnValues = (ArrayList<String>) entry.getValue();
                    // print value of each column
                    for (String value : columnValues) {
                        // dumb only projected values
                        if (value == null)
                            continue;
                        // if not first column value print comma
                        if (putComma) {
                            System.out.print(",");
                            output.print(",");
                        }
                        System.out.print(value);
                        output.print(value);
                        putComma = true;
                    }
                }
            // if we have select items print with this order
            } else {
                for (SelectItem item : selectItemList) {
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
                        // if not first column value print comma
                        if (putComma) {
                            System.out.print(",");
                            output.print(",");
                        }
                        System.out.print(tupleValues.get(tupleTable).get(columnPos));
                        output.print(tupleValues.get(tupleTable).get(columnPos));
                        putComma = true;
                }
            }
            newLine = true;
        }
        System.out.println();
        output.close();
    }
}