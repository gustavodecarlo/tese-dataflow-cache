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

    $ kind cluster create

## Airflow, spark-operator e as dags para as simulações

- Primeiro instalar e aplicar as permissões do spark-operator nas instruções: spark-operator/README.md

- Para o Airflow e as dag para as simulações as instruções para implementar e instalar o cenário, estão no diretório: airflow/README.md

## O que foi feito

- Arquitetura do cenário para a tese:
    - Cluster Kubernetes usando o kind
    - Airflow no cluster kubernetes
    - Spark Operator
    - Com dag de exemplo de como utilizar o operador do airflow com o spark operator.

## Próximos passos

- Definir onde serão armazenados os fragmentos e as operações, hipóteses:
    - Delta Lake no HDFS ou Filesystem local e Apache Cassandra para as operações
- Desenvolver a biblioteca que vai salvar o fragmento de dados e as operações do dataflow em pyspark
- Montar pipeline de teste da biblioteca do gerenciador do cache
- trabalhar em cenários propostos no relatório da disciplina de estudo dirigido


