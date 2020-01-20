# clickhouse-datasource-bridge

An alternative implementation of JDBC Bridge for ClickHouse.


## Features

* **Named Query**

    Besides data source, you can define named queries in configuration as well.

    Named data source - *config/datasources/ch.json*
    ```json
    {
        "ch": {
            "jdbcUrl": "jdbc:clickhouse://localhost/system",
            "dataSource.user": "default",
            "dataSource.password": "",
            "type": "jdbc",
            "parameters": {
                "max_rows": 1000,
                "fetch_size": 200
            }
        }
    }
    ```
    Note: both `type` and `parameters` are optional.

    Named query - *config/queries/test-query.json*
    ```json
    {
        "test-query": {
            "query": "select * from test_table",
            "columns": {
                "list": [
                    {
                        "name": "column1",
                        "type": "UInt32",
                        "nullable": false
                    },
                    {
                        "name": "column2",
                        "type": "String",
                        "nullable": true
                    },
                    {
                        "name": "column3",
                        "type": "Decimal",
                        "nullable": true,
                        "precision": 10,
                        "scale": 2
                    }
                ]
            },
            "parameters": {
                "max_rows": 10
            }
        }
    }
    ```
    Note: like named data source, `paramters` is not mandatory. `columns` is optional too but it's highly recommended, as it prevents runtime type inferring which could be slow.

* **Query Parameter**

    You can put query parameters like `max_rows` either in config or in the query.

    ```sql
    -- get the first 10 rows(based on above configuration)
    select * from jdbc('ch', '', 'test-query')
    -- get a specific row
    select * 
    from jdbc('ch?max_rows=3&offset=2', '',
        'select * from test-table order by column1 desc')
    ```

* **Multiple Types of Data Sources**

    In addition to JDBC, `clickhouse-datasource-bridge` is extensible to support arbitrary data sources.

    ```sql
    -- run named query in pre-defined database
    select * from jdbc('ch', '', 'test-query')
    -- get list of jobs from Jenkins view
    select * from jdbc('jenkins:https://builds.apache.org/', '', 'jobs')
    ```

* **Based on Vert.x**

    [Eclipse Vert.x](https://vertx.io) is event driven and non blocking. It also makes `clickhouse-datasource-bridge` easy to config and scale.


## Quick Start

```bash
# run with default configuration and JDBC drivers
docker run --rm -it -p 9019:9019 zhicwu/clickhouse-datasource-bridge

# run with custom configuration and JDBC drivers
docker run --rm -v `pwd`/config:/app/config -v `pwd`/drivers:/app/drivers -it -p 9019:9019 zhicwu/clickhouse-datasource-bridge
```


## Usage

```bash
ClickHouse client version 19.11.8.46 (official build).
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 19.11.8 revision 54423.

ch-server :) select * from jdbc('ch', '', 'select 1')

SELECT *
FROM jdbc('ch', '', 'select 1')

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.120 sec. 

ch-server :) select * from jdbc('jdbc:ch', '', 'select 1')

SELECT *
FROM jdbc('jdbc:ch', '', 'select 1')

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.074 sec. 

ch-server :) select * from jdbc('jdbc://ch', '', 'select 1')

SELECT *
FROM jdbc('jdbc://ch', '', 'select 1')

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.084 sec. 

ch-server :) select * from jdbc('jdbc://ch?debug=true', '', 'select 1')

SELECT *
FROM jdbc('jdbc://ch?debug=true', '', 'select 1')

┌─datasource─┬─type─┬─query────┬─parameters─────────────────────────────────────┐
│ ch         │ jdbc │ select 1 │ fetch_size=1000&max_rows=0&offset=0&position=0 │
└────────────┴──────┴──────────┴────────────────────────────────────────────────┘

1 rows in set. Elapsed: 0.052 sec. 

ch-server :) select * from jdbc('jdbc:clickhouse://localhost/system?user=default&password=', '', 'select 1')

SELECT *
FROM jdbc('jdbc:clickhouse://localhost/system?user=default&password=', '', 'select 1')

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.156 sec. 
```

## Configuration

Below is the directory structure used in docker image. By default, all configuration files must be put under `config` directory and JDBC drivers under `drivers`. 

```bash
/app
  |
  |--- config
  |     |
  |     |--- datasources
  |     |     |
  |     |     |--- test-mysql.json
  |     |
  |     |--- queries
  |     |     |
  |     |     |--- test-query.json
  |     |
  |     |--- vertx.json
  |     |--- server.json
  |
  |--- drivers
  |     |
  |     |--- some-shaded-jdbc-driver.jar
  |
  |--- clickhouse-datasource-bridge.jar
```

 Usually you don't need any of them but you can surely customize as needed.

| File               | Reloadable (Y/N) | Description |
| ------------------ | ---------------- | ----------- |
| vertx.json         | N                | Vertx configuration, check [here](https://github.com/eclipse-vertx/vert.x/blob/master/src/main/generated/io/vertx/core/VertxOptionsConverter.java) for more. |
| server.json        | N                | Server configuration. |
| datasources/*.json | Y                | Named data sources.   |
| queries/*.json     | Y                | Named queries.        |



* vertx.json

    This configuration file will be only read during server starting and it is NOT reloadable afterwards.

    ```json
    {
        "maxWorkerExecuteTime": 300,
        "maxWorkerExecuteTimeUnit": "SECONDS",
        "workerPoolSize": 10
    }
    ```

* server.json

    This configuration file will be only read during server starting and it is NOT reloadable afterwards.

    ```json
    {
        "serverPort": 8080,
        "requestTimeout": 5000,
        "queryTimeout": 60000
    }
    ```

* datasources/named-data-source.json

* datasources/named-query.json


## TODOs
- [ ] Enable write access
- [ ] Parallelized query
- [ ] More data sources...
