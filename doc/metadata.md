
*The DataSlice object contains extracted Data, it is enriched with metadata computed from query execution. Below is the metadata list and a note explaining how they are calculated.*


# Where will these metadata be available ? 
A DataSlice object will be returned  instead of DataFrame by each method that retrieve data, in order to provide access to these metadata everywhere. 

# Metadata list
* `input_parameters` dict: a dict which contains an extensible list of parameters extracted from user’s input for example

    * `limit` extracted from the query itself

    * `offset` extracted from the query itself

* `stats` DataStats, it contains various statistics computed from the query execution for example:

    * `total_rows` int total rows returned by the query. See details below.
    
    * `total_returned_rows` int total of rows returned by the executed query. This value is capped by the max number of rows retrievable by query (set at the instance level).

    * `execution_time` float, in milliseconds. See below for more details

    * `conversion_time` float: same

    * `df_memory_size` int the size of extracted data in bytes.

# Metadata computation
* `input_parameters`
It contains all parameters we were able to extract from input. In the context of a Snowflake (or SQL) data extraction, we implemented the extraction of limit & offset directly from the query using regular expressions.

* `Stats`
This object will collect various stats such as execution times, sizes, row numbers etc..

* `total_rows`
Check if the input “data” query is a select query

Build a “count” query from the datasource’s “data” query. The method wraps the initial query as such → `select count(*) from (<original-query>);`

Execute both queries (“data” & “count”) in parallel and store the result.

* `total_returned_rows`
This metric is retrieved using cursor.execute().rowcount 

* `df_memory_size`
Computed using pandas.DataFrame().memory_usage().sum() 

* `execution_time`
```
Measured this way:
execution_start = timer()
cursor = c.cursor(DictCursor)
query_res = cursor.execute(query, query_parameters)
...
execution_end = timer()
```
* `conversion_time`
Measured this way:
```
convert_start = timer()
....
values = pd.DataFrame.from_dict(query_res.fetch*())
....
convert_end = timer()
```
