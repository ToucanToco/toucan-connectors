#!/bin/bash

set -e

# Supports debian 12 and ubuntu 24.04

if [[ -e ~/mssql-installed ]]; then
    echo "MSSQL connector dependencies are already installed."
    exit
fi

apt-get update
apt-get install -fyq --no-install-recommends gnupg curl ca-certificates
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
source /etc/os-release

curl "https://packages.microsoft.com/config/${ID}/${VERSION_ID}/prod.list" \
    | tee /etc/apt/sources.list.d/mssql-release.list

apt-get update
ACCEPT_EULA=Y apt-get -y install msodbcsql18 unixodbc-dev

touch ~/mssql-installed
