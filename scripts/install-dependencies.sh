#!/bin/bash

set -eu
cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "Installing sbt"
if [[ ! -z $(which yum) ]]; then
    sudo rm -f /etc/yum.repos.d/bintray-rpm.repo
    curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo
    sudo mv sbt-rpm.repo /etc/yum.repos.d/
    sudo yum install -y sbt
elif [[ ! -z $(which apt-get) ]]; then
    sudo apt-get update
    sudo apt-get install apt-transport-https curl gnupg -yqq
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
    sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
    sudo apt-get update
    sudo apt-get install -y sbt
else
    echo "Operating system not supported, please install the dependencies manually"
fi

echo "Installing Pyenv"
if [ ! -d "${HOME}/.pyenv" ] && [ -n "${PYENV_ROOT-}" ]; then
    git clone https://github.com/pyenv/pyenv.git ${HOME}/.pyenv
    echo 'export PYENV_ROOT="${HOME}/.pyenv"' >> ${HOME}/.bashrc
    echo 'export PATH="${PYENV_ROOT}/bin:${PATH}"' >> ${HOME}/.bashrc
else
    echo "Pyenv is already installed"
fi

echo "Installing Pyenv virtualenv plugin"
if [ ! -d "${HOME}/.pyenv/plugins/pyenv-virtualenv" ]; then
    git clone https://github.com/pyenv/pyenv-virtualenv.git ${HOME}/.pyenv/plugins/pyenv-virtualenv
else
    echo "Pyenv virtualenv plugin is already installed"
fi

echo "Installing Pyenv's dependencies"
if [[ ! -z $(which yum) ]]; then
    sudo yum install -y patch
elif [[ ! -z $(which apt-get) ]]; then
    sudo apt-get install -y patch
else
    echo "Operating system not supported, please install the dependencies manually"
fi
