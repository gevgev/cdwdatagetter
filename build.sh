#!/bin/sh
set -x

GOOS=linux go build

rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi

GOOS=linux go build -v github.com/gevgev/aws-s3-uploader.git

rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi

docker build -t gevgev/cdw-hh-counter .

rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi

docker push gevgev/cdw-hh-counter

echo 'Success'
