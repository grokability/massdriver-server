#!/bin/sh

. .env


MESSAGE="{\"cmd\":\"$@\"}"
echo $MESSAGE
echo $@
echo "$@"
echo $*
echo "$*"


printf '<%s>\n' "$MESSAGE"
# exec
curl --fail --silent --show-error \
  --aws-sigv4 "aws:amz:${AWS_REGION}:sqs" \
  --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" \
  --header "X-Amz-Security-Token: ${AWS_SESSION_TOKEN:-}" \
  --data-urlencode 'Action=SendMessage' \
  --data-urlencode 'Version=2012-11-05' \
  --data-urlencode "MessageBody=${MESSAGE}" \
  --data-urlencode "MessageGroupId=brady" \
  --data-urlencode 'MessageAttribute.1.Name=subsystem' \
  --data-urlencode 'MessageAttribute.1.Value.DataType=String' \
  --data-urlencode 'MessageAttribute.1.Value.StringValue=cron' \
  "$SQS_QUEUE"