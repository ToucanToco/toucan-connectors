clickhouse-client --user ubuntu --password ilovetoucan --database=clickhouse_db --query="CREATE or replace TABLE clickhouse_db.city (
    id integer NOT NULL,
    name text NOT NULL,
    countrycode character(3) NOT NULL,
    district text NOT NULL,
    population integer NOT NULL
) ENGINE=Memory;"
