POSTGRES_IP=10.244.0.6
docker-build:
	docker build --tag airflow-w-dags:latest .

kind-image-import:
	kind load docker-image airflow-w-dags:latest

install-all:
	make docker-build
	make kind-image-import

helm-airflow-upgrade:
	helm upgrade airflow-tese apache-airflow/airflow --namespace default \
    	--set images.airflow.repository=airflow-w-dags \
    	--set images.airflow.tag=latest \
		--set data.metadataConnection.host=${POSTGRES_IP} \
		--set postgresql.enabled=false \
		--recreate-pods

helm-repo-postgresql-install:
	helm repo add cetic https://cetic.github.io/helm-charts
	helm repo update

helm-postgresql-install:
	helm install postgres-airflow cetic/postgresql

helm-airflow-install:
	 helm install airflow-tese apache-airflow/airflow --namespace default \
	 	--set images.airflow.repository=airflow-w-dags \
    	--set images.airflow.tag=latest \
		--set data.metadataConnection.host=${POSTGRES_IP} \
		--set postgresql.enabled=false

helm-airflow-uninstall:
	helm uninstall airflow-tese -n default