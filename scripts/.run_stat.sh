#!/usr/bin/env bash

##### VARIABLES
RUN_NAME="File-Download-Stat-$(date +"%Y")-$(date +"%b")-$((RANDOM % 9000 + 1000))"
LOG_FILE="${LOG_FOLDER}${RUN_NAME}_nextflow.log"
PROFILE=$1

${NEXTFLOW}nextflow -log $LOG_FILE run ${PIPELINE_BASE_DIR}main.nf \
              -name $RUN_NAME \
              -profile $PROFILE \
              --nextflow_location $NEXTFLOW \
              --root_dir $DATA_ROOT_DIR \
              --log_file $LOG_FILE