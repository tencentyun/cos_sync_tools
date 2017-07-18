#!/bin/bash
export LANG=en_US.utf8

cur_dir=$(cd `dirname $0`;pwd)
cp_path=${cur_dir}/src/main/resources/*:${cur_dir}/dep/*

java -cp "$cp_path" com.qcloud.cos.cos_sync.main.CosSyncMain
