# dbt-mds

preprequisites to ran a demo:
1. s3 localstack - go to localstack subdir and run ```docker compose up -d```
2. airbyte - refer to [airbyte local deployment](https://docs.airbyte.com/deploying-airbyte/local-deployment/)
3. clickhose - ```docker run -d -p 18123:8123 -p19000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server```


architecture:
![image](https://user-images.githubusercontent.com/13641958/224531827-aeea33d5-afea-4f02-9bb4-f76526cf5916.png)


imdb movies data assets lineage:
![image](https://user-images.githubusercontent.com/13641958/224531275-c7b6c40e-be30-4e96-9af0-6fbd7953e269.png)
