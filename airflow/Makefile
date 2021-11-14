docker-build:
	docker build --tag airflow-w-dags:latest .

k3d-image-import:
	k3d image import airflow-w-dags:latest -c k8s-tese

docker-all:
	make docker-build
	make k3d-image-import