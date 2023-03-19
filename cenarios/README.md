spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,io.delta:delta-core_2.12:1.0.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.4,io.delta:delta-contribs_2.12:1.0.0 --executor-memory 4g --num-executors 2 --driver-memory 8g covid_pipelines/main.py covid-raw --source-data gs://lncc-tese-datafrag/source_covid/csse_covid_19_daily_reports --table covid_raw

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,io.delta:delta-core_2.12:1.0.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.4,io.delta:delta-contribs_2.12:1.0.0 --executor-memory 4g --num-executors 2 --driver-memory 8g covid_pipelines/main.py cleaned-and-agg-covid --source-data gs://lncc-tese-datafrag/covid_brazil/covid_raw --table covid_treated_agg --filter 'Country_Region="Brazil"'

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,io.delta:delta-core_2.12:1.0.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.4,io.delta:delta-contribs_2.12:1.0.0 --executor-memory 4g --num-executors 2 --driver-memory 8g covid_pipelines/main.py cleaned-and-agg-covid --source-data gs://lncc-tese-datafrag/covid_brazil/covid_raw --table covid_treated_agg --filter 'Country_Region="Brazil"' --have-datafrag


spark-submit --conf "fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" --conf "fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" --executor-memory 2g --num-executors 1 --driver-memory 3g covid_pipelines/main.py covid-raw --source-data gs://lncc-tese-datafrag/source_covid/csse_covid_19_daily_reports --table covid_raw

## containment test commands

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,io.delta:delta-core_2.12:2.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.4,io.delta:delta-contribs_2.12:2.1.1 --executor-memory 4g --num-executors 2 --driver-memory 8g covid_pipelines/main.py containment-dummy-raw --source-data $HOME/bigdata/data_test/source_data --table dummy_raw

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,io.delta:delta-core_2.12:2.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.4,io.delta:delta-contribs_2.12:2.1.1 --executor-memory 4g --num-executors 2 --driver-memory 8g covid_pipelines/main.py cleaned-and-filter-containment --source-data $HOME/bigdata/data_test/containment/dummy_raw --table dummy_treat_filter --filter '((A >= 5 AND A <= 10) AND ((B >= 2 AND B <= 6) OR (C >= 10 AND C <= 15)))' --have-datafrag

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,io.delta:delta-core_2.12:2.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.4,io.delta:delta-contribs_2.12:2.1.1 --executor-memory 4g --num-executors 2 --driver-memory 8g covid_pipelines/main.py test-containment --source-data $HOME/bigdata/data_test/containment/dummy_raw --filter '((A#1 >= 6 AND A#1 <= 8) AND ((B#2 = 6) OR (C#3 >= 11 AND C#3 <= 13)))'


kubectl port-forward --namespace default svc/cassandra 9042:9042
cqlsh -u cassandra -p fkJH6FhnkE