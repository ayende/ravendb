#!/bin/bash

DEFPATH=$( cd `dirname $0` && pwd )
SERVER_DIR="$DEFPATH/Server"
RAVEN_DEBUG_PATH="$SERVER_DIR/Raven.Debug"

if [[ $UID != 0 ]]; then
    echo "This script needs to be run as root."
    echo "Try: sudo $0"
    exit 1
fi

echo "Enabling debugging for RavenDB..."

UBUNTU_LIBC6PKG='libc6-dev'

# if there's dpkg and apt (Ubuntu) we try to install it
# On CentOS 7 it just works out of the box 
if ! command -v dpkg >/dev/null; then
    echo "NOTE: Please make sure appropriate glibc is installed on your OS."
else
    dpkg -l $UBUNTU_LIBC6PKG | grep -q ^.i >/dev/null 2>&1
    if [[ $? -ne 0 ]]; then
        installed=1

        if command -v "apt" > /dev/null; then
            echo -n "Installing $UBUNTU_LIBC6PKG package using apt..."
            apt -qq update >/dev/null 2>&1
            apt -qq -y install $UBUNTU_LIBC6PKG >/dev/null 2>&1
            installed=0
        elif command -v "apt-get" > /dev/null; then
            echo -n "Installing $UBUNTU_LIBC6PKG package using apt-get..."
            apt-get -qq update >/dev/null 2>&1
            apt-get -y -qq install $UBUNTU_LIBC6PKG >/dev/null 2>&1
            installed=0
        fi

        if [[ $installed != 0 ]]; then
            echo "Error installing $UBUNTU_LIBC6PKG."
            echo "Please install $UBUNTU_LIBC6PKG manually. Exiting..."
            exit 3
        else
            echo "DONE"
        fi
    fi
fi

binList=("$RAVEN_DEBUG_PATH")
for binFilePath in "${binList[@]}"
do
    binFilename="$(basename $binFilePath)"

    echo -n "Adjust $binFilename binary permissions..."
    if [[ ! -f "$binFilePath" ]]; then
        echo "$binFilename binary not found in under $binFilePath. Exiting..."
        exit 5
    fi

    setcap cap_sys_ptrace=eip "$binFilePath"
    chown root:root "$binFilePath"
    chmod +s "$binFilePath"
    echo "DONE"
done

echo "RavenDB debugging has been enabled."

