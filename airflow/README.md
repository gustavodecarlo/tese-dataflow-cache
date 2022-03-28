# Airflow

Será utilizado para fazer a orquestração e simulação dos dataflows com a camada do cache.

## Adicionar o chart

    $ helm repo add apache-airflow https://airflow.apache.org
    $ helm repo update

## Instalar a ultima versão do airflow python 3.7 no cluster k8s

    $ helm install airflow-tese apache-airflow/airflow --namespace default

## Atualizar o docker registry com as imagem do Airflow com as dags

    $ make install-all

## Atualizar o airflow com as dags para a simulação

As dags para as simulações dos dataflows precisam estar na pasta `dags` do diretório `airflow`. Então utilizar o comando abaixo para enviar as dags para o airflow no cluster k8s.

    $ make helm-airflow-upgrade

## Para expor o serviço do airflow

    $ kubectl port-forward svc/airflow-tese-webserver 8080:8080 --namespace default