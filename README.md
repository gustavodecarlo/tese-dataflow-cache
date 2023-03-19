# tese-dataflow-cache

## Requisitos

- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [helm](https://helm.sh/)
- [spark-operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)
- [airflow](https://airflow.apache.org/)
- [kubernetes](https://kubernetes.io/)
- [python >= 3.7](https://www.python.org/)
- [pyspark](http://spark.apache.org/docs/latest/api/python/)

## Criação do cluster k8s com o kind para o cenário da tese

    $ kind create cluster

## Airflow, spark-operator e as dags para as simulações

- Primeiro instalar e aplicar as permissões do spark-operator nas instruções: spark-operator/README.md

- Para o Airflow e as dag para as simulações as instruções para implementar e instalar o cenário, estão no diretório: airflow/README.md

- Para instalar o Cassandra para armazenar os metadados dos fragmentos, então no diretório: cassandra/README.md

## O que foi feito

- Arquitetura do cenário para a tese:
    - Cluster Kubernetes usando o kind
    - Airflow no cluster kubernetes
    - Spark Operator
    - Apache Cassandra
    - Com dag de exemplo de como utilizar o operador do airflow com o spark operator.
    - Para o cenário da tese: GCS com o delta lake (fragmentos) e Cassandra (Operaçoes) 
    - Desenvolver a biblioteca que vai salvar o fragmento de dados e as operações do dataflow em pyspark

## Próximos passos

- Testar dentro do Airflow o containment com cenário dummy e covid
