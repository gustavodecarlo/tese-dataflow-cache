FROM python:3.9

ARG CERT_MOZILLA_URL=https://curl.haxx.se/ca/cacert.pem
ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz
ARG SCALA_URL=https://downloads.lightbend.com/scala/2.12.15/scala-2.12.15.deb
ARG POETRY_VERSION=1.2.0

ENV DEBIAN_FRONTEND=noninteractive

ARG spark_uid=185

RUN apt-get update \
&&  apt-get install -y \
    libssl-dev \
    wget \
    curl \
    bash \
    tini \
    libc6 \
    libpam-modules \
    libpam-krb5 \
    krb5-user \
    libnss3 \
    procps \
    default-jdk \
&&  rm -rf /var/lib/apt/lists/* \
&&  rm -rf /var/cache/* \
&&  mkdir -p /etc/pki/tls/certs \
&&  mkdir -p /opt/spark \
&&  mkdir -p /opt/spark/work-dir \
&&  touch /opt/spark/RELEASE \
&&  rm /bin/sh \
&&  ln -sv /bin/bash /bin/sh \
&&  echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su \
&&  chgrp root /etc/passwd && chmod ug+rw /etc/passwd \
&&  rm -rf /var/cache/apt/* \
&&  wget -O /etc/pki/tls/certs/ca-bundle.crt ${CERT_MOZILLA_URL} \
&&  wget -O /opt/spark.tgz ${SPARK_URL} \
&&  wget -O /opt/scala.deb ${SCALA_URL} \
&&  tar -xvf /opt/spark.tgz -C /opt/spark --strip-components=1 \
&&  dpkg -i /opt/scala.deb \
&&  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
&&  rm -fr /opt/*.tgz \
&&  rm -f /opt/*.deb

ENV PATH $PATH:/opt/java/bin:/root/.cargo/bin
ENV SPARK_HOME /opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PYSPARK_PYTHON=/usr/local/bin/python

# Setup dependencies for Google Cloud Storage access.
RUN rm $SPARK_HOME/jars/guava-14.0.1.jar
ADD https://repo1.maven.org/maven2/com/google/guava/guava/31.0.1-jre/guava-31.0.1-jre.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.9/gcs-connector-hadoop3-2.2.9-shaded.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.23.2/spark-bigquery-with-dependencies_2.12-0.23.2.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.2.0/spark-cassandra-connector_2.12-3.2.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-driver_2.12/3.2.0/spark-cassandra-connector-driver_2.12-3.2.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core-shaded/4.12.0/java-driver-core-shaded-4.12.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/datastax/oss/java-driver-shaded-guava/25.1-jre-graal-sub-1/java-driver-shaded-guava-25.1-jre-graal-sub-1.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/datastax/oss/java-driver-query-builder/4.12.0/java-driver-query-builder-4.12.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/datastax/oss/java-driver-mapper-runtime/4.12.0/java-driver-mapper-runtime-4.12.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/datastax/oss/native-protocol/1.5.1/native-protocol-1.5.1.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/thoughtworks/paranamer/paranamer/2.8/paranamer-2.8.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/typesafe/config/1.4.2/config-1.4.2.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.1.0/delta-core_2.12-2.1.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/2.1.0/delta-storage-2.1.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/io/delta/delta-contribs_2.12/2.1.0/delta-contribs_2.12-2.1.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/datastax/oss/java-driver-mapper-processor/4.12.0/java-driver-mapper-processor-4.12.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/reactivestreams/reactive-streams/1.0.3/reactive-streams-1.0.3.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/github/jnr/jnr-posix/3.1.15/jnr-posix-3.1.15.jar $SPARK_HOME/jars

RUN chmod 644 $SPARK_HOME/jars/*.jar

RUN jar -xvf $SPARK_HOME/jars/zstd-jni-1.5.0-4.jar linux/aarch64/libzstd-jni-1.5.0-4.so \
    && mv linux/aarch64/libzstd-jni-1.5.0-4.so /usr/lib \
    && rm -fr linux/

RUN pip install "poetry==${POETRY_VERSION}"
RUN poetry config virtualenvs.create false

WORKDIR /opt/spark/work-dir
RUN chmod g+w /opt/spark/work-dir

ENTRYPOINT ["/opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh"]

COPY datafrag-manager datafrag-manager
COPY cenarios cenarios

RUN cd cenarios/cenario_od_covid \
    && poetry install --only main --no-root \
    && cd /opt/spark/work-dir

USER ${spark_uid}
