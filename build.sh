#!/bin/bash


if [ -d "docker_images" ]
then
    echo "Directory docker_images exist and result will be saved to directory."
else
    echo "Creating new Directory docker_images, and result will be saved to directory."
    mkdir -p ./docker_images/releases
fi



VERSION=$(python setup.py --version | sed 's/+/./g')
IMAGE_FULL_NAME=automation-sync-test:$VERSION
OUTPUT_DIR=/docker_images

echo VERSION:$VERSION
docker build --no-cache --rm -t automation-sync-test:$VERSION --build-arg VERSION=$VERSION .
#docker build -t automation-test:$VERSION --build-arg VERSION=$VERSION .

docker tag automation-sync-test:$VERSION automation-sync-test:latest

echo automation-sync-test:$VERSION > ./docker_images/generated_dockers.txt


# path build.sh with "DUMP_IMAGE=1" to save docker
FILE_NAME=automation-sync-test:${VERSION}.tar
DUMP_OUTPUT_PATH=${OUTPUT_DIR}/releases/${FILE_NAME}

if [[ ! -z "${DUMP_IMAGE}" ]]
then
     docker save -o ./${DUMP_OUTPUT_PATH} automation-sync-test:$VERSION
     echo Saved docker image file into:${DUMP_OUTPUT_PATH}
fi
