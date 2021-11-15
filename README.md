# tese-dataflow-cache

## Requisitos

- [k3d](https://k3d.io/v5.1.0/)
- [helm](https://helm.sh/)
- [spark-operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)
- [airflow](https://airflow.apache.org/)
- [kubernetes](https://kubernetes.io/)
- [python >= 3.7](https://www.python.org/)
- [pyspark](http://spark.apache.org/docs/latest/api/python/)

## Criação do cluster k8s com o k3d para o cenário da tese

    $ k3d cluster create k8s-tese

## Para executar o stop no Cluster k8s

Aqui recomendado para o cluster não ficar consumindo os recursos do ambiente local.

    $ k3d cluster stop k8s-tese

## Para o start no Cluster k8s

Caso tenha parado o cluster, este comando realizar o start.

    $ k3d cluster start k8s-tese

## Airflow, spark-operator e as dags para as simulações

- Primeiro instalar e aplicar as permissões do spark-operator nas instruções: spark-operator/README.md

- Para o Airflow e as dag para as simulações as instruções para implementar e instalar o cenário, estão no diretório: airflow/README.md

