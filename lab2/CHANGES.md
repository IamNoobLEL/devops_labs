# Что изменилось после lab №1

- Добавлен `spark-master` и `spark-worker`
- В Airflow добавлены Java, Spark provider и `pyspark`
- DAG запускает Spark джобу через `SparkSubmitOperator`
- Отчет считается через `SparkSession`