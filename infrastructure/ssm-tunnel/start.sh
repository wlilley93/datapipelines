#!/usr/bin/env sh
set -e

: "${AWS_REGION:?Missing AWS_REGION}"
: "${SSM_TARGET:?Missing SSM_TARGET}"
: "${SSM_HOST:?Missing SSM_HOST}"

SSM_REMOTE_PORT="${SSM_REMOTE_PORT:-5432}"
SSM_LOCAL_PORT="${SSM_LOCAL_PORT:-5433}"
SSM_EXPOSE_PORT="${SSM_EXPOSE_PORT:-5432}"

aws ssm start-session \
  --region "$AWS_REGION" \
  --target "$SSM_TARGET" \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters "{\"host\":[\"$SSM_HOST\"],\"portNumber\":[\"$SSM_REMOTE_PORT\"],\"localPortNumber\":[\"$SSM_LOCAL_PORT\"]}" &
SSM_PID=$!

trap 'kill "$SSM_PID" 2>/dev/null || true' INT TERM EXIT

exec socat TCP-LISTEN:"$SSM_EXPOSE_PORT",fork,reuseaddr TCP:127.0.0.1:"$SSM_LOCAL_PORT"
