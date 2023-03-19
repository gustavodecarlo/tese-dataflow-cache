# Cassandra

Será utilizado para fazer o armazenar as operações dos fragmentos de dados da camada do cache.

## Instalar a última versão do cassandra no cluster k8s

    $ kubectl apply -f cassandra_stateful.yml

## Configurar o KEYSPACE para as operações

Necessário entrar no pod do cassandra e segue o comando de criação:

```
CREATE KEYSPACE dataflow_operation
WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};

USE dataflow_operation;
CREATE TABLE datafrag_operations(
   dataflow text PRIMARY KEY,
   datasource text,
   operation text,
   timestamp timestamp
);

CREATE TABLE datafrag_containment(
   dataflow text,
   datasource text,
   attribute text,
   task text,
   term bigint,
   min double,
   max double,
   PRIMARY KEY ((datasource, attribute, term), dataflow)
);
 
```

pyspark --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,io.delta:delta-core_2.12:1.0.0 \
--conf spark.cassandra.connection.host=127.0.0.1 \
--conf spark.cassandra.auth.password=UL7kopfcWq \
--conf spark.cassandra.auth.username=cassandra \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,com.datastax.spark.connector.CassandraSparkExtensions \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

