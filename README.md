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


gcloud beta container --project "gdecarlo-cloud-tests" clusters create "cluster-tese-foresee" --zone "us-central1-c" --no-enable-basic-auth --cluster-version "1.22.11-gke.400" --release-channel "regular" --machine-type "e2-standard-2" --image-type "COS_CONTAINERD" --disk-type "pd-standard" --disk-size "40" --metadata disable-legacy-endpoints=true --scopes "https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" --max-pods-per-node "110" --num-nodes "3" --logging=SYSTEM,WORKLOAD --monitoring=SYSTEM --enable-ip-alias --network "projects/gdecarlo-cloud-tests/global/networks/default" --subnetwork "projects/gdecarlo-cloud-tests/regions/us-central1/subnetworks/default" --no-enable-intra-node-visibility --default-max-pods-per-node "110" --no-enable-master-authorized-networks --addons HorizontalPodAutoscaling,HttpLoadBalancing,GcePersistentDiskCsiDriver --enable-autoupgrade --enable-autorepair --max-surge-upgrade 1 --max-unavailable-upgrade 0 --enable-shielded-nodes --node-locations "us-central1-c"

## Próximos passos

- Atualizar a api de fragmentos com python puro para usar na dag do airflow
- TDD da api de fragmentos spark
- Montar pipeline de teste da biblioteca do gerenciador do cache
- trabalhar em cenários propostos no relatório da disciplina de estudo dirigido
    - inciar com o cenário do COR da disciplina cb500 de bigdata
    - Montar um cenário com os dados da covid do John Hopikns

