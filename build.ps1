docker build -t lambda-go-builder .

docker create --name tmp-lambda lambda-go-builder
docker cp tmp-lambda:/function.zip ./function.zip
docker rm tmp-lambda


awslocal lambda delete-function --function-name test-go-lambda 2>$null

$outputcreate = awslocal lambda create-function `
  --function-name test-go-lambda `
  --runtime provided.al2 `
  --handler bootstrap `
  --zip-file fileb://function.zip `
  --role arn:aws:iam::000000000000:role/lambda-role `
  --timeout 600 `
  --environment "Variables={AWS_USE_PATH_STYLE=true,AWS_ENDPOINT_URL=http://host.docker.internal:4566}" `
  --region us-east-1;$output = awslocal lambda invoke --function-name test-go-lambda --cli-binary-format raw-in-base64-out --payload file://payload.json output.txt; Get-Content output.txt


