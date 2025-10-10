
docker build -t lambda-test .

docker create --name tmp-container lambda-test
docker cp tmp-container:/app/bootstrap C:\Workspace\Docker\cron\output\bootstrap
docker rm tmp-container

powershell -Command "Compress-Archive -Path output\bootstrap -DestinationPath .\function.zip"

zip function.zip bootstrap

awslocal lambda create-function --function-name test-go-lambda --runtime provided.al2 --zip-file fileb://function.zip --handler bootstrap --role arn:aws:iam::000000000000:role/lambda-role --region us-east-1 --environment Variables="{AWS_USE_PATH_STYLE=true,AWS_ENDPOINT_URL=http://host.docker.internal:4566}"


$output = awslocal lambda invoke --function-name test-go-lambda --cli-binary-format raw-in-base64-out  --payload file://payload.json output.txt; Get-Content output.txt

awslocal lambda update-function-configuration --function-name test-go-lambda --environment "Variables={AWS_USE_PATH_STYLE=true,AWS_ENDPOINT_URL=http://host.docker.internal:4566}"




pip install  --platform manylinux2014_x86_64 --target=python --implementation cp  --python-version 3.8  --only-binary=:all: --upgrade psycopg2-binary -t package

format D: /fs:ntfs /p:4


awslocal --endpoint-url=http://localhost:4566 s3 mb s3://testbucket


awslocal lambda create-function \
--function-name localstack-go-lambda-example \

--runtime go1.x \
--handler bootstrap \
--zip-file fileb://function.zip \
--role arn:aws:iam::000000000000:role/lambda-role


C:\Users\tarik\AppData\Local\Programs\Python\Python313\python.exe -m pip install awscli-local


GOOS=linux GOARCH=amd64 go build -o bootstrap main.go
zip function.zip bootstrap

set GOOS=linux
set GOARCH=amd64
go build -o bootstrap main.go

go build --platform=linux/amd64 -o bootstrap main.go
C:\Users\tarik\sdk\go1.24.1\bin\go build --platform=linux/amd64 -o bootstrap main.go



awslocal events put-rule --cli-input-json file://rule.json

awslocal events list-rules

awslocal lambda add-permission --function-name test-go-lambda --statement-id eventbridge-invoke --action "lambda:InvokeFunction" --principal events.amazonaws.com --source-arn arn:aws:events:us-east-1:000000000000:rule/DailyLambdaFunction

awslocal events put-targets --rule DailyLambdaFunction --targets file://targets.json


awslocal events list-targets-by-rule --rule DailyLambdaFunction

"ScheduleExpression": "cron(55 10 * * ? *)"
"ScheduleExpression": "rate(2 minutes)"


awslocal events remove-targets --rule DailyLambdaFunction --ids "1"
awslocal events delete-rule --name DailyLambdaFunction





awslocal ssm put-parameter --name "/ceptesok/fulfillment/qa/aurora_postgres/cluster_endpoint" --value "host.docker.internal" --type SecureString --overwrite
awslocal ssm put-parameter --name "/ceptesok/fulfillment/qa/aurora_postgres/cluster_port" --value "5432" --type SecureString --overwrite
awslocal ssm put-parameter --name "/ceptesok/fulfillment/qa/aurora_postgres/cluster_master_username" --value "postgres" --type SecureString --overwrite
awslocal ssm put-parameter --name "/ceptesok/fulfillment/qa/aurora_postgres/cluster_master_password" --value "1" --type SecureString --overwrite


awslocal s3 mb s3://scripts --region us-east-1
awslocal s3 mb s3://logs --region us-east-1

awslocal s3api put-object --bucket scripts --key scripts/order_archive_daily.sql --body "C:\Workspace\Docker\cron\order_archive_daily.sql"
awslocal s3api put-object --bucket scripts --key scripts/order_archive_daily.sql --body "C:\Workspace\Docker\cron\vacuum.sql"
awslocal s3api put-object --bucket scripts --key scripts/order_archive_daily.sql --body "C:\Workspace\Docker\cron\transaction.sql"
awslocal s3api put-object --bucket scripts --key scripts/order_archive_daily.sql --body "C:\Workspace\Docker\cron\daily_status.sql"


awslocal s3 ls s3://scripts/ --recursive
awslocal s3 ls s3://logs/ --recursive

awslocal s3 rm s3://logs/ --recursive
awslocal s3 rb s3://logs/ 
