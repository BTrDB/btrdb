#!/bin/bash

set -ex

git clone https://github.com/grpc-ecosystem/grpc-gateway

go generate

rm -rf grpc-gateway
