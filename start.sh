#!/bin/bash

nohup hive2elastic_post > /dev/stdout 2>&1 &
nohup hive2elastic_account > /dev/stdout 2>&1
