FROM apache/airflow:2.2.5-python3.9

ARG POETRY_VERSION=1.1.13

COPY dags dags
COPY plugins plugins
COPY datafrag-manager datafrag-manager

RUN pip install "poetry==${POETRY_VERSION}"
RUN poetry config virtualenvs.create false
RUN cd datafrag-manager \
    && pip install dist/*.whl \
    && cd ..
