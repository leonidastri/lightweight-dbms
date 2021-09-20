# lightweight-dbms

A lightweight database management system

***

### Database Object

This is an object to keep the necessary information for the database input. We keep in a map the files and their locations. In another nested map, we keep the schema with tables as keys and a map of columns and their positions as values (Check **Database.java**).

### Tuple

We created a java file to help us manage tuples. A tuple consists of a map which has tables as keys and lists with the column values as values. This is useful as we always know which tables the tuple contains and the values of each column of its table (Check **Tuple.java**).

### Aliases Map

This is a LinkedHashMap which has aliases as keys and their real tables as values. This is a useful map which we use in different parts of the program to get the real tables from aliases and then scan their columns using the Database Object.

### Visitor

In the file **Visitor.java**, the value of where cluase expression is evaluated and a true or false value is returned. This class is used whenever we want to check if a tuple satsfies an expression and needs to be returned as a result or in another parent operator.

### ConjuctVisitor

The file **ConjuctVisitor.java** is used to split the while clause of the query. We split and store the where clause in smaller expressions. We have a map in which we store tables/aliases or joined tables/aliases as keys and their where expressions in which the table/aliases participate. For example if we have the following query:

**Select * FROM Sailors, Boats WHERE Sailors.A > 2 AND Boats.E = 1 AND
Sailors.B = Boats.F**

We get the following map result:

**{Sailors: {Sailors.A > 2}, Boats: {Boats.E = 1}, SailorsBoats = {Sailors.B = Boats.F}}**

This is useful as we can use specific where expressions in the points of the program we want in order to evaluate which tuples satisfy the expressions.

### QueryPlan
In QueryPlan.java I create the operators tree. I tried to write a method for each operator which can be found in the file. In the end of the createQueryPlan method the root operator of the query plan is returned to **QueryParser.java** in order to call the dumb method of the root operator to output the result of the query.

## Operators

### Scan Operator

It operates a full scan in the table given. Scan operator constitutes the operator who opens the table file to get each tuple.

### Select Operator

In the getNextTuple() method for each tuple returned from the child operator (Scan Operator), the expression of the table (check **ConjuctVisitor section**) is evaluated to check if tuple satisfies it and if yes it is returned.

### Project Operator

It is used after scan and select operators and before join, sort and duplicateEliminator operator to project tuples using the SelectItems of the query. If SelectItems is * then all values of tuples are projected. Check **ProjectOperator.java** to see the logic of how it was implemented. After Project Operator, each Tuple object contains all tables and the projected values have values, whereas all other values become null. By doing this, after Project Operator we run the next operators with specific values in order to optimize the performance of our program as it is not necessary to use all values of tuples, but only the projected values.

### Join Operator

Join Operator was also implemented as it was described. In the getNextTuple method I implemented the nested loop join algorithm learned in the lectures. You can check the logic in **JoinOperator.java**. The general logic is that for every tuple of the left child of the join I get each tuple of the right child of the join and check if the joined tuple satisfies the specific join expression constructed in the **ConjuctVisitor.java**. Check also **QueryPlan.java** to see how we implement the left-join-tree (Check **createJoinOp method**).

### Sort Operator

You can check the logic of the implementation of sort operator in SortOperator.java as there are comments. In order to sort tuples of the output, we used Comparator class. We wrote a new compare method in which if there are ORDER BY items then these are used first to compare the tuples, else we get all other values of tuples one by one to sort tuples. Please check the sort method found in **SortOperator.java**. This method is called first for all the tuples of the output and then getNextTuple method of sort operator is called to get each tuple.

### DuplicateEliminator Operator

This is the last operator of the query plan and it only exists if there is the Distinct word in the query. If there is no ORDER BY in the query then the output needs to be sorted first by creating a sort operator and then check if there are duplicates. The process of doing that is to return in the result only tuples which have values (projected values only as project operator was used before to keep only
those values) different from the previous printed tuple (Check **DuplicateEliminator.java** for the logic).
