#!/usr/bin/env bash
# ******************************************************
# DESC    : pika port build script
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : Apache License 2.0
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2019-01-22 19:54
# FILE    : build.sh
# ******************************************************

# install gflags
git clone https://github.com/gflags/gflags.git
cd gflags
git checkout v2.0
./configure && make && sudo make install
cd ..

# install compression lib
sudo yum install -y snappy snappy-devel
sudo yum install -y zlib zlib-devel
sudo yum install -y bzip2 bzip2-devel
sudo yum install -y lz4-devel

# download third libs
# pls wait for about 30 minutes
git submodule update --init --recursive --force

# compile pika-port
cd pika_port && make