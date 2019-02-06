# Auditor's benchmark for DynamoDB

## How to start

1. Download DynamoDB.
1. Launch the server:
    ```
    java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb
    ```
1. Get deps:
    ```
    go get -u ./...
    ```
1. Create tables.
1. Run the benchmark.

## DynamoDB aws-cli

```
aws dynamodb describe-table --table-name transactions  --endpoint-url http://localhost:8000
```

```
aws dynamodb update-table --table-name transactions --endpoint-url http://localhost:8000 help
```