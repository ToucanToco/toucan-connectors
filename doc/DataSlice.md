# DataSlice object

The DataSlice object contains a slice (or page) of  extracted data enriched with metadata computed from query execution.

This object is meant to be used when designing a query on the Toucan Toco platform, hence the metadata (to infrom and guide a query designer) and the limited pages of results (to avoid overloading the client with large datasets).

Below is the metadata list and a note explaining how they are calculated.

## DataSlice attributes

* `input_parameters` (`dict`): contains an extensible list of parameters extracted from user’s input for example:
    
    * `limit` extracted from the query itself
    * `offset` extracted from the query itself

* `stats` (`DataStats`), it contains various statistics computed from the query execution for example:

    * `total_rows` (`int`): total rows returned by a user query.
    * `total_returned_rows` (`int`): total of rows returned in a slice of results.
    * `execution_time` (`float`): in milliseconds, query execution time.
    * `conversion_time` (`float`): in milliseconds, time to convert results in a `pandas.DataFrame`.
    * `df_memory_size` (`int`): size of extracted data in bytes.

## DataSlice attributes computation in Snowflake connector

* `input_parameters`:
 contains all parameters we were able to extract from input. In the context of a Snowflake (or SQL) data extraction, we implemented the extraction of limit & offset directly from the query using regular expressions.

* `stats`: this object collects various stats such as execution times, sizes, row numbers etc..

    * `total_rows`: 
        * Check if the input “data” query is a select query
        * build a “count” query from the datasource’s “data” query. The method wraps the initial query as such → `select count(*) from (<original-query>);` 
        * Execute both queries (“data” & “count”) in parallel and store the result.
    * `total_returned_rows`: this metric is retrieved using cursor.execute().rowcount
    * `df_memory_size`: computed using pandas.DataFrame().memory_usage().sum()
    * `execution_time`: 
    ```
    execution_start = timer()
    cursor = c.cursor(DictCursor)
    query_res = cursor.execute(query, query_parameters)
    ...
    execution_end = timer()
    ```

    * `conversion_time`

    ```
    convert_start = timer()
    ....
    values = pd.DataFrame.from_dict(query_res.fetch*())
    ....
    convert_end = timer()
    ```

