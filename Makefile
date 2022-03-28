docker-build:
	docker build --tag tese-spark:latest .

kind-image-import:
	kind load docker-image tese-spark:latest

install-all:
	make docker-build
	make kind-image-import