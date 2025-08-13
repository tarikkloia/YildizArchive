# 1. Docker'da Go binary build et
docker build -t lambda-go-builder .

# 2. bootstrap binary'yi çıkar
docker create --name tmp-lambda lambda-go-builder
docker cp tmp-lambda:/function.zip ./function.zip
docker rm tmp-lambda


# 4. LocalStack Lambda fonksiyonunu sil (önceki varsa)
awslocal lambda delete-function --function-name test-go-lambda 2>$null

# 5. Fonksiyonu yükle
$outputcreate = awslocal lambda create-function `
  --function-name test-go-lambda `
  --runtime provided.al2 `
  --handler bootstrap `
  --zip-file fileb://function.zip `
  --role arn:aws:iam::000000000000:role/lambda-role `
  --timeout 600 `
  --region us-east-1;$output = awslocal lambda invoke --function-name test-go-lambda --cli-binary-format raw-in-base64-out --payload file://payload.json output.txt; Get-Content output.txt


