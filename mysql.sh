#!/bin/sh -e
nslookup "$MYSQL_HOST"
while ! nc -z "$MYSQL_HOST" "$MYSQL_PORT"; do
  sleep 1
done
echo "MySQL ready"
