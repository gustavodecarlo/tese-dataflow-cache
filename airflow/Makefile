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
		--recreate-pods