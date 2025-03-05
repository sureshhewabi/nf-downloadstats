#!/usr/bin/env bash

set -a  # Automatically export all variables
source .env
set +a  # Stop automatically exporting variables

##### VARIABLES
DATA_ROOT_DIR=$LOGS_DESTINATION_ROOT #"/path/to/data/transfer_logs_privacy_ready"
RUN_NAME="File-Download-Stat-$(date +"%Y")-$(date +"%b")-$((RANDOM % 9000 + 1000))"
LOG_FILE="${RUN_NAME}_nextflow.log"
API_BASE_URL=""
API_ENDPOINT_FILE_DOWNLOADS_PER_PROJECT=""
API_ENDPOINT_FILE_DOWNLOADS_PER_FILE=""
API_ENDPOINT_HEADER=""

PROFILE=$1
conda activate file_download_stat
nextflow -log $LOG_FILE run ${PIPELINE_BASE_DIR}main.nf \
              -params-file params/$RESOURCE_NAME-$PROFILE-params.yml \
              -name $RUN_NAME \
              -profile $PROFILE \
              --root_dir $DATA_ROOT_DIR \
              --log_file $LOG_FILE \
              --api_endpoint_file_downloads_per_project $API_ENDPOINT_FILE_DOWNLOADS_PER_PROJECT \
              --api_endpoint_file_downloads_per_file $API_ENDPOINT_FILE_DOWNLOADS_PER_FILE \
              --api_endpoint_header $API_ENDPOINT_HEADER -resume
#              -with-dag workflow_dag.png \
