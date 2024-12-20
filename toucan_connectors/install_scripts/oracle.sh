#!/bin/bash -x
set -e

if [[ -e ~/oracle-installed ]]; then
    echo "Oracle connector dependencies are already installed"
    exit
fi

apt-get update

source /etc/os-release
LIBAIO=""
if [[ "$ID" == "debian" ]]; then
    LIBAIO="libaio1"
    apt-get install -fyq --no-install-recommends \
        curl unzip ca-certificates "$LIBAIO"
else
    # Ubuntu 24.04 migrated to 64 bits components for most libs, which does not
    # work with the oracle library drivers
    LIBAOI="libaio1t64"
    apt-get install -fyq --no-install-recommends \
        curl unzip ca-certificates "$LIBAIO"
    # WARNING: not safe: Creating a symbolic link from the 64bits version to the
    # 32bits one. A better fix would probably be to switch to
    # https://oracle.github.io/python-oracledb/ eventually
    ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 \
        /usr/lib/x86_64-linux-gnu/libaio.so.1
fi

mkdir -p /opt/oracle
curl -sSL 'https://public-package.toucantoco.com/connectors_sources/oracle/oracle_client_lib/instantclient-basiclite-linux.x64-12.2.0.1.0.zip' -o '/tmp/oracle_client_lib.zip'
unzip /tmp/oracle_client_lib.zip -d /opt/oracle
sh -c "echo /opt/oracle/instantclient_12_2 > /etc/ld.so.conf.d/oracle-instantclient.conf"
ldconfig
rm -f /tmp/oracle_client_lib.zip

touch ~/oracle-installed
