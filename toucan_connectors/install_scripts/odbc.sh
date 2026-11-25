#!/bin/bash

set -e

if [[ -e ~/odbc-installed ]]; then
    echo "odbc driver already installed"
    exit
fi

# packages.microsoft.com ships its own unixODBC builds (e.g. odbcinst1debian2 2.3.11-3 in its
# debian/11 repo). When that repo is configured on a newer distro, apt prefers them over the
# distro's packages, and dpkg fails because they overwrite files owned by the distro's
# libodbc2/libodbcinst2/unixodbc-common. Always install unixODBC from the distro instead.
cat > /etc/apt/preferences.d/unixodbc-from-distro <<'PREF'
Package: unixodbc unixodbc-dev unixodbc-common odbcinst odbcinst1debian2 libodbc1 libodbc2 libodbcinst2 libodbccr2
Pin: origin packages.microsoft.com
Pin-Priority: 1
PREF

apt-get update
apt-get -fyq install build-essential unixodbc-dev odbc-postgresql

touch ~/odbc-installed
