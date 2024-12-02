#!/usr/bin/env bash

##### VARIABLES
RUN_NAME="File-Download-Stat-$(date +"%Y")-$(date +"%b")-$((RANDOM % 9000 + 1000))"
LOG_FILE="${LOG_FOLDER}${RUN_NAME}_nextflow.log"
PROFILE=$1
API_ENDPOINT_FILE_DOWNLOAD_PER_PROJECT=$2
API_ENDPOINT_HEADER=$3

nextflow -log $LOG_FILE run ${PIPELINE_BASE_DIR}main.nf \
              -params-file params/pride-$PROFILE-params.yml \
              -name $RUN_NAME \
              -profile $PROFILE \
              --nextflow_location "" \
              --root_dir $DATA_ROOT_DIR \
              --log_file $LOG_FILE \
              --api_endpoint_file_download_per_project $API_ENDPOINT_FILE_DOWNLOAD_PER_PROJECT \
              --api_endpoint_header $API_ENDPOINT_HEADER