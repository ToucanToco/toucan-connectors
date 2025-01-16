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

MSSQL_INSTALLER_PATH="$(dirname $0)/mssql.sh"
exec $MSSQL_INSTALLER_PATH
