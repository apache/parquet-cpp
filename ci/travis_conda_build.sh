#!/usr/bin/env bash

set -e

if [ $TRAVIS_OS_NAME == "linux" ]; then
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh"
else
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda-latest-MacOSX-x86_64.sh"
fi

wget -O miniconda.sh $MINICONDA_URL
MINICONDA=$HOME/miniconda
bash miniconda.sh -b -p $MINICONDA
export PATH="$MINICONDA/bin:$PATH"

conda update -y -q conda
conda install -y -q conda-build
conda info -a

conda config --set show_channel_urls yes
conda config --add channels conda-forge
conda config --add channels apache

conda install --yes jinja2 anaconda-client

cd $TRAVIS_BUILD_DIR

conda build conda.recipe

CONDA_PACKAGE=`conda build --output conda.recipe | grep bz2`

if [ $TRAVIS_BRANCH == "master" ] && [ $TRAVIS_PULL_REQUEST == "false" ]; then
  anaconda --token $ANACONDA_TOKEN upload $CONDA_PACKAGE --user apache --channel dev;
fi
