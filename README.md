# Cassandra Data Copy Tool
This simple java based tool copies data from a live cassandra table to another. The source and destination tables do not need to be on the same cluster or keyspace. All you need to ensure is that the destination table is compatible with the source table. 

## How to use it

### Build

The project requires:

* Java 8
* Maven 2 or later versions to run. 

Once you've got your environment setup run from the project directory.

> mvn package

This builds a jar in ./target/cassandra-data-copy-tool-<VERSION>-SNAPSHOT.jar"

### Run

Once you have your property file ready, simply run:

> java -jar cassandra-data-copy-tool-<VERSION>-SNAPSHOT.jar --spring.config.location=/path/to/config.properties

### Configuration

To configure the tool you'll need to create a properties file. Here's an example:

```
copy.tables=table1,table2,table3=>other_table4,...,tableN
copy.ignoreColumns=tab1e1.columnX,table2.columnY
copy.batchSize=20000
copy.queryPageSize=1000
copy.batchesPerSecond=1

source.cassandra.contactPoints=127.0.0.1
source.cassandra.port=9142
source.cassandra.keyspace=test_keyspace
source.cassandra.username=cassandra
source.cassandra.password=cassandra

destination.cassandra.contactPoints=127.0.0.1
destination.cassandra.port=9142
destination.cassandra.keyspace=test_keyspace
destination.cassandra.username=cassandra
destination.cassandra.password=cassandra
```

#### Properties

There are three configuration groups:

* Copy
* Source
* Destination

##### Copy

Property Name | Description | Default Value 
------------ | ------------- | -------------
copy.tables | A column delimited list of table names to copy. If the table names don't match between source and destination use source_table=>dest_table | ""  
copy.ignoreColumns | A comma delimited list of columns from the source to ignore. Format is TABLE_NAME.COLUMN_NAME | ""
copy.batchSize | Size of batches to insert into destination database. | 20000
copy.queryPageSize | Size of pages as read from source | 1000
copy.batchesPerSecond | Maximum rate of batches copied per second | 1

##### Source

Property Name | Description | Default Value 
------------ | ------------- | -------------
source.cassandra.contactPoints | Comma delimited list of source cluster's contact points | 127.0.0.1
source.cassandra.port | Source cluster's port | 9142
source.cassandra.keyspace | Source keyspace name | ""
source.cassandra.username | Source username | cassandra
source.cassandra.password | Source plaintext password | cassandra

##### Destination

Property Name | Description | Default Value 
------------ | ------------- | -------------
destination.cassandra.contactPoints | Comma delimited list of destination cluster's contact points | 127.0.0.1
destination.cassandra.port | Destination cluster's port | 9142
destination.cassandra.keyspace | Destination keyspace name | ""
destination.cassandra.username | Destination username | cassandra
destination.cassandra.password | Destination plaintext password | cassandra
