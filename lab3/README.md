# Lab №3

## Как поднять

```bash
cd lab3/gitlab-runner
docker compose up -d
docker ps
```

- Airflow UI: http://localhost:8082
- Spark UI: http://localhost:4041

Логин и пароль Airflow:

```text
airflow / airflow
```

## Пруфы что все воркает

Линк на репозиторий: https://gitlab.com/bogsava123/devops_lab3 

Картинки:

![docker ps](screenshots/docker_ps.png)

![runner online](screenshots/runners.jpg)

![successful pipeline](screenshots/succesful_pipeline.jpg)

![test job](screenshots/test_job.jpg)

![build job](screenshots/build_job.jpg)

![deploy job](screenshots/deploy_job.jpg)

Feauture ветка ждет ручного подтверждения:
![feature branch example](screenshots/feature_test_example.jpg)