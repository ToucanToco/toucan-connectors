#!/bin/bash
set -e

if [[ -e ~/mssql-installed ]]; then
    echo "MSSQL connector dependencies are already installed."
    exit
fi

apt-get update
apt-get install -fyq gnupg curl
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

source /etc/os-release
if [ "$ID" == "debian" ]; then
    # debian/12 fails - fixing to debian/11 works:
    curl "https://packages.microsoft.com/config/debian/11/prod.list" \
        | tee /etc/apt/sources.list.d/mssql-release.list
else
    curl "https://packages.microsoft.com/config/${ID}/${VERSION_ID}/prod.list" \
        | tee /etc/apt/sources.list.d/mssql-release.list
fi
apt-get update
ACCEPT_EULA=Y apt-get -y install msodbcsql17 unixodbc-dev

touch ~/mssql-installed
