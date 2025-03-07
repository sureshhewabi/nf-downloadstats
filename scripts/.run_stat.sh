#!/usr/bin/env bash

##### VARIABLES
RUN_NAME="nf-downloadstat-$(echo "$RESOURCE" | tr '[:upper:]' '[:lower:]')-$(date +"%Y")-$(date +"%b" | tr '[:upper:]' '[:lower:]')-$((RANDOM % 9000 + 1000))"
LOG_FILE="${LOG_FOLDER}${RUN_NAME}_nextflow.log"
RESOURCE=$1
PROFILE=$2
API_ENDPOINT_FILE_DOWNLOADS_PER_PROJECT=$3
API_ENDPOINT_FILE_DOWNLOADS_PER_FILE=$4
API_ENDPOINT_HEADER=$5

nextflow -log $LOG_FILE run ${PIPELINE_BASE_DIR}main.nf \
              -params-file params/$RESOURCE-$PROFILE-params.yml \
              -name $RUN_NAME \
              -profile $PROFILE \
              --nextflow_location "" \
              --root_dir $DATA_ROOT_DIR \
              --log_file $LOG_FILE \
              --api_endpoint_file_downloads_per_project $API_ENDPOINT_FILE_DOWNLOADS_PER_PROJECT \
              --api_endpoint_file_downloads_per_file $API_ENDPOINT_FILE_DOWNLOADS_PER_FILE \
              --api_endpoint_header "$API_ENDPOINT_HEADER" -resume -with-tower