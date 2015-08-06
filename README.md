# Hive UDF's


## Build the jar

```
mvn package
```

## Run

```
%> hive
hive> ADD JAR hive-udfs-0.1.jar;
hive> create temporary function row_sequence as 'io.woolford.hive.udf.UDFRowSequence';
hive> select row_sequence(), col1 from some_table;

```
