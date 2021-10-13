# Airflow

Será utilizado para fazer a orquestração e simulação dos dataflows com a camada do cache.

Referência docker airflow: https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

## Requisitos

- [docker](https://www.docker.com/get-started)
- [docker-compose](https://docs.docker.com/compose/)

Segue abaixo para iniciar o Airflow

    $ make airflow-start

Para pausar a aplicação e remover os componentes:

    $ make airflow-stop

## As Dags para simulação

As dags para as simulações dos dataflows precisam estar na pasta `dags` do diretório `airflow`. Com isso o docker-compose irá mapear para o container que vai iniciar a aplicação.