# Sql to MongoDB Converter Package


## Introduction

This package is designed to translate SQL commands into their equivalent MongoDB operations. It is particularly useful
for applications migrating from relational databases to NoSQL databases or for systems that need to support both query
languages concurrently.

At its core is the QueryConverter class, which coordinates the query conversion process. It leverages various **processors**
to handle different SQL constructs. Each processor is specialized in transforming a specific part of the SQL query into
MongoDB query.


> # ⚠️ WARNING ⚠️
>
> This library was hastily written and contains numerous inefficiencies and poor design choices. Critical errors may occur, and overall, the implementation is far from ideal.
> It is based on an older library rather than being entirely my own creation. The original version was outdated, so I attempted to improve it.
> If you have the option to use an alternative, please do so. Even if you don’t, I still strongly advise against using this library...
>
> **Please do not use this...** 🥺


## Query Conversion Flow

1. [**QueryConverter.convert(...)**](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/converters/QueryConverter.java#L89)
   - Starts the conversion process with an input SQL `Statement`.

2. [**SQLCommandInfo.fromStatement(...)**](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/holders/SQLCommandInfo.java#L94)
   - Parses the SQL statement.
   - Determines the type of command (Select, Delete, Update, Insert).
   - Extracts details and creates an [`SQLCommandInfo`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/holders/SQLCommandInfo.java) object.

3. [**QueryConverter.getMongoQueryInternal(...)**](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/converters/QueryConverter.java#L158)
   - Creates a [`MongoQueryHolder`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/query/MongoQueryHolder.java) to store MongoDB query components.
   - Executes the *query processing pipeline* using registered processors.

4. [**QueryProcessor**](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/QueryProcessor.java) **Instances**:

   - [`FromSubQueryProcessor`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/FromSubQueryProcessor.java)
   - [`DistinctProcessor`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/DistinctProcessor.java)
   - [`GroupByProcessor`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/GroupByProcessor.java)
   - [`TotalGroupProcessor`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/TotalGroupProcessor.java)
   - [`JoinProcessor`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/JoinProcessor.java)
   - [`ProjectionProcessor`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/ProjectionProcessor.java)
   - [`CountAllProcessor`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/CountAllProcessor.java)
   - [`WhereProcessor`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/WhereProcessor.java)
   - [`HavingProcessor`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/HavingProcessor.java)
   - [`OrderByProcessor`](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/processors/OrderByProcessor.java)

5. [**MongoQueryHolder**](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/query/MongoQueryHolder.java) (Updated by processors):
   - Stores the final MongoDB query components:

        - Filter-related [components](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/query/components/MongoQueryFilters.java)
        - Projection-related [components](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/query/components/MongoQueryProjection.java)
        - Aggregation-related [components](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/query/components/MongoQueryAggregation.java)
        - Update-related [components](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/query/components/MongoQueryUpdate.java)

6. [**MongoQueryFormatter.formatQuery(...)**](https://github.com/m-i-k-o-r/sql-2-mongo-converter/blob/master/src/main/java/com/koroli/queryconverter/utils/MongoQueryFormatter.java#L28)
    - Formats the query components into a valid MongoDB query string.

7. **Return MongoDB Query**
   - The fully converted query is returned as a string.
