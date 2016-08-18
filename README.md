# slow_query_exporter

A daemon for exporting MySQL slow query logs to Graylog. It tails the MySQL
slow query log and sends each query to Graylog as a GELF packet.

This is pre-alpha and not tested on production. Cleanup and documentation
will hopefully be forthcoming.

## Configuration

slow_query_exporter has several settings which you can provide in a config
file:

```
LogPath = "/var/lib/mysqllogs/mysql-slow.log"
GraylogHost = "localhost"
GraylogPort = 12201
```

The config file location can be specified with the `-c` option:

```
$ slow_query_exporter -c my_config /path/to/slow_query.log
```
