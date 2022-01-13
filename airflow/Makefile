docker-build:
	docker build --tag airflow-w-dags:latest .

kind-image-import:
	kind load docker-image airflow-w-dags:latest

k3d-image-import:
	k3d image import airflow-w-dags:latest -c k8s-tese

install-all:
	make docker-build
	make kind-image-import