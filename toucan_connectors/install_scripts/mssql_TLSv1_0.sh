#!/bin/bash
set -e

if grep -q 'TLSv1' /etc/ssl/openssl.cnf; then
    if grep -q 'TLSv1.2' /etc/ssl/openssl.cnf; then
      sed -i 's/DEFAULT@SECLEVEL=2/DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf
      sed -i 's/TLSv1.2/TLSv1.0/g' /etc/ssl/openssl.cnf
    fi
else
    echo "[system_default_sect]" >> /etc/ssl/openssl.cnf
    echo "MinProtocol = TLSv1.0" >> /etc/ssl/openssl.cnf
    echo "DEFAULT@SECLEVEL=1" >> /etc/ssl/openssl.cnf
fi


if [[ -e ~/mssql-installed ]]; then
    echo "MSSQL connector dependencies are already installed."
    exit
fi

apt-get update
apt-get install -fyq gnupg curl
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
source /etc/os-release &&\
    curl "https://packages.microsoft.com/config/${ID}/${VERSION_ID}/prod.list" \
    | tee /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get -y install msodbcsql17 unixodbc-dev

touch ~/mssql-installed
