mackerel-plugin-aws-dynamodb
=================================

AWS DynamoDB custom metrics plugin for mackerel.io agent.

## Synopsis

```shell
mackerel-plugin-aws-dynamodb -table-name=<table-name> -region=<aws-region> [-access-key-id=<id>] [-secret-access-key=<key>] [-tempfile=<tempfile>]
```
* collect data from specified AWS DynamoDB
* you can set keys by environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

## Example of mackerel-agent.conf

```
[plugin.metrics.aws-dynamodb]
command = "/path/to/mackerel-plugin-aws-dynamodb"
```
