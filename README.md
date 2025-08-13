
docker build -t lambda-test .

docker create --name tmp-container lambda-test
docker cp tmp-container:/app/bootstrap C:\Workspace\Docker\cron\output\bootstrap
docker rm tmp-container

powershell -Command "Compress-Archive -Path output\bootstrap -DestinationPath .\function.zip"

zip function.zip bootstrap

awslocal lambda create-function --function-name localstack-lambda-url-example --runtime provided.al2 --zip-file fileb://function.zip --handler bootstrap --role arn:aws:iam::000000000000:role/lambda-role --region us-east-1


awslocal lambda invoke --function-name localstack-lambda-url-example ./out.txt

$output = awslocal lambda invoke --function-name test-go-lambda output.txt;Get-Content output.txt
$output = awslocal lambda invoke --function-name test-go-lambda --payload "{\"args\": [\"5\", \"destination_db\", \"order_archive_daily.sql\", \"order_archive_daily\"]}"  output.txt;Get-Content output.txt
$output = awslocal lambda invoke --function-name test-go-lambda --cli-binary-format raw-in-base64-out --payload file://payload.json output.txt; Get-Content output.txt




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