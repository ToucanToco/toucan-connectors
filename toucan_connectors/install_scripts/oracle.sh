#!/bin/bash
set -e

if [[ -e ~/oracle-installed ]]; then
    echo "Oracle connector dependencies are already installed"
    exit
fi

apt-get update
apt-get install -fyq libaio1 curl wget unzip
mkdir -p /opt/oracle
curl -s 'https://public-package.toucantoco.com/connectors_sources/gdown/gdown.pl' -o /tmp/gdown.pl
chmod +x /tmp/gdown.pl
/tmp/gdown.pl 'https://public-package.toucantoco.com/connectors_sources/oracle/oracle_client_lib/oracle_client_lib.zip' '/tmp/oracle_client_lib.zip'
unzip /tmp/oracle_client_lib.zip -d /opt/oracle
sh -c "echo /opt/oracle/instantclient_12_2 > /etc/ld.so.conf.d/oracle-instantclient.conf"
ldconfig
rm -rf /tmp/gdown.pl /tmp/oracle_client_lib.zip

touch ~/oracle-installed
