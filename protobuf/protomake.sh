#!/bin/bash
#go get -u github.com/gogo/protobuf/{proto,protoc-gen-gogo,gogoproto,protoc-gen-gofast}
#export PATH=$PATH:$GOPATH/bin
#protoc --gofast_out=sendrecv sendrecv.proto

#  go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
#sudo apt install golang-protobuf-extensions-dev
mkdir -p sendrecv
protoc --go_out=sendrecv sendrecv.proto
