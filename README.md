# clickhouse-datasource-bridge

An alternative implementation of JDBC Bridge for ClickHouse.


## Features

* **Named Query**

    Besides data source, you can define named queries in configuration as well.

    Named data source - *config/datasources/ch.json*
    ```json
    {
        "ch": {
            "type": "jdbc",
            "jdbcUrl": "jdbc:clickhouse://localhost/system",
            "dataSource": {
                "user": "default",
                "password": "",
            },
            "columns": [
                { "name": "instance_id", "type": "Int32", "value": "0", "nullable": false }
            ],
            "parameters": {
                "max_rows": 1000,
                "fetch_size": 200
            }
        }
    }
    ```
    Note: `type`, `columns` and `parameters` are optional.

    Named query - *config/queries/test-query.json*
    ```json
    {
        "test-query": {
            "query": "select * from test_table",
            "columns": [
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
            ],
            "parameters": {
                "max_rows": 10
            }
        }
    }
    ```
    Note: like named data source, `paramters` is not mandatory. `columns` is optional too but it's highly recommended, as it prevents runtime type inferring which could be slow.

    Saved query - *scripts/tests/test-query.sql*
    ```sql
    select * from test_table
    ```
    
    With above configuration setup, you should be able to run the following queries in ClickHouse:
    ```sql
    -- adhoc query
    select * from jdbc('ch', 'select * from test_table');

    -- named query
    select * from jdbc('ch', 'test-query');

    -- saved query
    select * from jdbc('ch', 'scripts/tests/test-query.sql');
    ```

* **Query Parameter**

    You can put query parameters like `max_rows` either in config or in your query.

    ```sql
    -- get the first 10 rows(based on above configuration)
    select * from jdbc('ch', 'test-query')
    -- replace all null values
    select * from jdbc('ch?null_as_default=true', 'test-query')
    -- get a specific row
    select * from jdbc('ch?max_rows=3&offset=2', 'select * from test_table order by column1 desc')
    -- retrieve meta data for defining a named query
    select * from jdbc('ch?debug=true', 'select * from test_table limit 1')
    ```

* **SRV Record Support**

    If you're using Consul or any other DNS server with SRV record support, you probably want to use service name instead of hostname/IP and port number combination when defining a datasource. To do that, assuming `mysql.service.dc1.consul` is the service name pointing to `127.0.0.1:3306`, you can use any of below format instead of `jdbc:mysql://127.0.0.1:3306/test`:
    * `jdbc:mysql://{{ mysql.service.dc1.consul }}/test`
    * `jdbc:mysql://{{ host:mysql.service.dc1.consul }}/test`
    * `jdbc:mysql://{{ host:mysql.service.dc1.consul }}:{{ port:mysql.service.dc1.consul }}/test`

* **Multiple Types of Data Sources**

    In addition to JDBC, `clickhouse-datasource-bridge` is extensible to support arbitrary data sources.

    ```sql
    -- run named query in pre-defined database
    select * from jdbc('ch', 'test-query')
    -- get list of jobs from Jenkins view
    select * from jdbc('jenkins:https://builds.apache.org/', 'jobs')
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

ch-server :) select * from jdbc('ch', 'select 1')

SELECT *
FROM jdbc('ch', 'select 1')

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.120 sec. 

ch-server :) select * from jdbc('jdbc:ch', 'select 1')

SELECT *
FROM jdbc('jdbc:ch', 'select 1')

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.074 sec. 

ch-server :) select * from jdbc('jdbc://ch', 'select 1')

SELECT *
FROM jdbc('jdbc://ch', 'select 1')

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.084 sec. 

ch-server :) select * from jdbc('jdbc://ch?debug=true', 'select 1')

SELECT *
FROM jdbc('jdbc://ch?debug=true', 'select 1')

┌─datasource─┬─type─┬─query────┬─parameters─────────────────────────────────────┐
│ ch         │ jdbc │ select 1 │ fetch_size=1000&max_rows=0&offset=0&position=0 │
└────────────┴──────┴──────────┴────────────────────────────────────────────────┘

1 rows in set. Elapsed: 0.052 sec. 

ch-server :) select * from jdbc('jdbc:clickhouse://localhost/system?user=default&password=', 'select 1')

SELECT *
FROM jdbc('jdbc:clickhouse://localhost/system?user=default&password=', 'select 1')

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
  |     |--- httpd.json
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
| httpd.json         | N                | Http server configuration, check [here](https://github.com/eclipse-vertx/vert.x/blob/master/src/main/generated/io/vertx/core/http/HttpServerOptionsConverter.java) for more. |
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


## Known Issues

| Issue              | Workaround       | Remark      |
| ------------------ | ---------------- | ----------- |
| query timed out    | * server-side: 1) increase timeout in datasource configuration; 2) increase `max_execution_timeout` in `server.json`; * client-side: 1) increase clickhouse-jdbc-driver timeout |  |


## TODOs
- [ ] Consul support - retrieve datasource and/or query from Consul KV store...
- [ ] Error handling - abort worker thread once client closed stream(exception from stream.write?)
- [ ] Special datasource like config - 'select * from jdbc('config', '', 'show datasources')'
- [ ] Special run-time parameter to save query and/or generate datasources(select * from jdbc('ds?save_query=xxx', '', 'select 1'))
- [ ] Strict mode(turn on by default)
- [ ] Provider for CodeQL
- [ ] More examples (docker-compose.yml, swarm and k8s, with/without Vert.x clustering)
- [ ] Reduce dependencies so that we can go Native - https://vertx.io/blog/eclipse-vert-x-goes-native/
    * Exclude unnecessary dependencies from Vertx lib
    * Simple LRU cache(better in JDK) to replace Caffeine
    * Avoid reflection - less configuration required to build native image
- [ ] Reduce memory usage if we stick with JVM - https://stackoverflow.com/questions/561245/virtual-memory-usage-from-java-under-linux-too-much-memory-used
- [ ] Parallelized query
- [ ] More data sources(Cassandra, DGraph, InfluxDB and Prometheus)...
