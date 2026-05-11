# Что изменилось относительно lab №3

- Добавлены сервисы Loki, Alloy, Prometheus и Grafana
- Добавлен конфиг `alloy.conf` для чтения логов из примонтированных папок
- Добавлен конфиг `prometheus.yml` для сбора метрик Spark
- Добавлен конфиг `spark-conf/metrics.properties` с `PrometheusServlet`