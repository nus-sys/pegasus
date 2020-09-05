#!/bin/bash

DIR=$(dirname $0)

$DIR/../p4/netcache/controller/controller.sh $DIR/netcache.json 32
