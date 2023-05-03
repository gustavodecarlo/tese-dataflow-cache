FROM apache/airflow:2.5.3-python3.9

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         git \
         build-essential \
         cmake \
         default-jdk \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && chown -R airflow /tmp

USER airflow

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH $PATH:/home/airflow/.cargo/bin
ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT 300

COPY dags dags
COPY plugins plugins
COPY datafrag-manager datafrag-manager

# RUN pip install "poetry==${POETRY_VERSION}"
# RUN poetry config virtualenvs.create false
USER root

RUN cd datafrag-manager \
    && /home/airflow/.local/bin/pip install --no-cache-dir dist/*.whl \
    && cd .. \
    && cp -Rf /root/.local/lib/python3.9/site-packages/* /home/airflow/.local/lib/python3.9/site-packages/ \
    && chown -R airflow /home/airflow/.local/lib/python3.9/site-packages/
