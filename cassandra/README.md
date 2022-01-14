# Cassandra

Será utilizado para fazer o armazenar as operações dos fragmentos de dados da camada do cache.

## Adicionar o chart

    $ helm repo add bitnami https://charts.bitnami.com/bitnami
    $ helm repo update

## Instalar a última versão do cassandra no cluster k8s

    $ helm install k8s-cassandra bitnami/cassandra

## Configurar o KEYSPACE para as operações

Necessário entrar no pod do cassandra e segue o comando de criação:

```
CREATE KEYSPACE dataflow_operation
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

USE dataflow_operation;
CREATE TABLE datafrag_operations(
   dataflow text PRIMARY KEY,
   operation text,
   timestamp timestamp
);
```

