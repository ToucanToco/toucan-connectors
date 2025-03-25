#!/bin/bash

set -e

if [[ -e ~/odbc-installed ]]; then
    echo "odbc driver already installed"
    exit
fi

apt-get update
apt-get -fyq install build-essential unixodbc-dev odbc-postgresql
