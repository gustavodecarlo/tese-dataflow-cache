# Spark Operator

Após o cluster kubernetes iniciado abaixo seguem as roles e rbac para serem aplicadas para que o contexto do airflow e com o resource customizado do spark operator.

## Instalar o spark-operator no cluster k8s

    $ helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
    $ helm repo update
    $ helm install spark-operator-release spark-operator/spark-operator --namespace spark-operator --create-namespace --set webhook.enable=true
    $ --set image.repository=spark-operator-arm --set image.tag=latest

## Cluster Roles

    $ kubectl apply -f cluster-roles.yml

## RBAC para jobs Spark

    $ kubectl apply -f spark-rbac.yml