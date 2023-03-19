# Airflow

Será utilizado para fazer a orquestração e simulação dos dataflows com a camada do cache.

## Adicionar o chart

    $ helm repo add apache-airflow https://airflow.apache.org
    $ helm repo update

## Instalar a ultima versão do airflow python 3.7 no cluster k8s

    $ helm install airflow-tese apache-airflow/airflow --namespace default --set data.metadataConnection.host=[postgres_ip] --set postgresql.enabled=false

## Atualizar o docker registry com as imagem do Airflow com as dags

    $ make install-all

## Atualizar o airflow com as dags para a simulação

As dags para as simulações dos dataflows precisam estar na pasta `dags` do diretório `airflow`. Então utilizar o comando abaixo para enviar as dags para o airflow no cluster k8s.

    $ make helm-airflow-upgrade

## Para expor o serviço do airflow

    $ kubectl port-forward svc/airflow-tese-webserver 8080:8080 --namespace default

## Para configurar o airflow na área das variáveis

Segue abaixo o exemplo do json de configuração.

```json
{
    "google_application_credentials": "",
    "warehouse": "",
    "cassandra_host": "",
    "cassandra_port": "",
    "google_bucket": "",
    "datafrag_warehouse": "",
    "datafrag_metatable": "",
    "datafrag_tc_metatable": "",
    "datafrag_keyspace": ""
}

{
    "google_application_credentials": "/opt/spark/work-dir/cenarios/cenario_od_covid/key.json",
    "warehouse": "covid_brazil",
    "cassandra_host": "10.244.0.8",
    "cassandra_port": "9042",
    "google_bucket": "lncc-tese-datafrag",
    "datafrag_warehouse": "gs://lncc-tese-datafrag/datafrag_test",
    "datafrag_metatable": "datafrag_operations",
    "datafrag_tc_metatable": "datafrag_containment",
    "datafrag_keyspace": "dataflow_operation"
}
```