#!/usr/bin/bash
# This script gets the basic development environment setup on WRDS
# It's main steps are getting the right version of anaconda
# The right version sqlite interface (APSW)
# And adding the necessary sqlite extensions.

#download anaconda into a scratch folder
echo "Setting up environmet"
export SCRATCH=/scratch/$GROUP/$USER
export REPFOLDER=$SCRATCH/Repfolder
mkdir $SCRATCH
mkdir $SCRATCH/downloads
mkdir $REPFOLDER
cd $SCRATCH/downloads

echo "Beginning to install necessary tools like python"

#download anaconda
wget https://repo.continuum.io/archive/Anaconda2-5.0.0.1-Linux-x86_64.sh 
bash Anaconda2-5.0.0.1-Linux-x86_64.sh #install anaconda

#GET PARALLELS, definitely need this
wget http://mirrors.peers.community/mirrors/gnu/parallel/parallel-20170922.tar.bz2 
tar xvjf parallel-20170922.tar.bz2
./configure --prefix=$HOME/anaconda2
make
make install

PATH=$HOME/anaconda2/bin:$PATH

#make sure the packages we need will be installed 
pip install --user https://github.com/rogerbinns/apsw/releases/download/3.20.1-r1/apsw-3.20.1-r1.zip \
--global-option=fetch --global-option=--version --global-option=3.20.1 --global-option=--all \
--global-option=build --global-option=--enable-all-extensions

pip install fasteners


export CPPFLAGS="-I$HOME/anaconda2/include"
export LDFLAGS="-L$HOME/anaconda2/lib"
export LIBRARY_PATH="$HOME/anaconda2/lib"
wget -c "https://sqlite.org/contrib/download/extension-functions.c/download/extension-functions.c?get=25" -O extension-functions.c
gcc -fPIC -lm  -shared -L$LD_LIBRARY_PATH -I$HOME/anaconda2/include extension-functions.c -o $SCRATCH/downloads/libsqlitefunctions.so
cp libsqlitefunctions.so $HOME

wget -c "https://www.sqlite.org/src/raw/ext/misc/csv.c?name=1a009b93650732e22334edc92459c4630b9fa703397cbb3c8ca279921a36ca11" -O $SCRATCH/downloads/csv.c
gcc -fPIC -lm -shared  -I$HOME/anaconda2/include csv.c -o csv.so

clear
echo "Environment is set, aacquiring data"

wget https://www.dropbox.com/sh/53ngc6h7cs6ycmr/AAC8nHWT5bT-pK_IUZs74sOCa?dl=1 -O frd.zip
unzip frd.zip -d $REPFOLDER

wget https://www.dropbox.com/s/ieq80o0k22anxxt/Handcoding%20Stocks_Nov1_2017.xlsx?dl=1 -O $REPFOLDER/Handmade\ Data/participant_stocks.xlsx

clear
echo "All downloads complete... starting analysis"

