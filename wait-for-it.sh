#!/bin/sh
# wait-for-it.sh

host="$1"
port="$2"
shift 2
cmd="$@"

echo "Waiting for $host:$port..."
until nc -z "$host" "$port"; do
  echo "MySQL not ready, sleeping..."
  sleep 2
done

echo "MySQL ready, executing command."
exec $cmd
