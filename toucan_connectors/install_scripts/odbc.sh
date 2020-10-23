#!/bin/bash

set -e

if [[ -e ~/odbc-installed ]]; then
    echo "odbc driver already installed"
    exit
fi

apt-get update
apt-get -fyq install wget build-essential odbc-postgresql
cd /tmp/
wget http://www.unixodbc.org/unixODBC-2.3.9.tar.gz
tar xvf /tmp/unixODBC-2.3.9.tar.gz
cd /tmp/unixODBC-2.3.9
./configure
make
make install
export LD_LIBRARY_PATH=/usr/local/lib/

if [[ ! -e "/usr/lib/psqlodbcw.so" ]];then
    echo "Driver file doesn't exist copying";
    cp /usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so /usr/lib/psqlodbcw.so;
fi




touch ~/odbc-installed
