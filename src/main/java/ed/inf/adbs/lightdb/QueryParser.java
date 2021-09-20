package ed.inf.adbs.lightdb;

import java.io.FileReader;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

/**
 * QueryParser Class
 *
 * -Parse the query-
 */
public class QueryParser {

	/**
	 * Parse query and create output
	 *
	 * Analytical steps of the process:
	 * 1. Parse query from Input Query
	 * 2. Create query plan
	 * 3. Get output of query plan
	 */
	public void parseQuery(String inputFile, String outputFile) {
        try {

            Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));

			if (statement != null) {
				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				System.out.println("Select body is " + select.getSelectBody());

				// create query plan
				QueryPlan queryPlan = new QueryPlan();
				Operator op = queryPlan.createQueryPlan(select);

				// find result of query and write it to output file
				op.dump(outputFile);
			}

		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
    }
}
