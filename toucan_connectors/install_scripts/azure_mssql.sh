#!/bin/bash
set -e

if [[ -e ~/azure_mssql-installed ]]; then
    echo "Azure MSSQL connector dependencies are already installed."
    exit
fi

apt-get update
apt-get install -fyq curl
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
source /etc/os-release &&\
    curl "https://packages.microsoft.com/config/${ID}/${VERSION_ID}/prod.list" \
    | tee /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get -y install msodbcsql17 unixodbc-dev

touch ~/azure_mssql-installed
