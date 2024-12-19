#!/bin/bash
set -e

if [[ -e ~/oracle-installed ]]; then
    echo "Oracle connector dependencies are already installed"
    exit
fi

apt-get update
apt-get install -fyq curl unzip
mkdir -p /opt/oracle
curl -sSL 'https://public-package.toucantoco.com/connectors_sources/oracle/oracle_client_lib/instantclient-basiclite-linux.x64-12.2.0.1.0.zip' -o '/tmp/oracle_client_lib.zip'
unzip /tmp/oracle_client_lib.zip -d /opt/oracle
sh -c "echo /opt/oracle/instantclient_12_2 > /etc/ld.so.conf.d/oracle-instantclient.conf"
ldconfig
rm -f /tmp/oracle_client_lib.zip

touch ~/oracle-installed
