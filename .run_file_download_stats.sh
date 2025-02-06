#!/usr/bin/env bash

# Load environment (and make the slrum command available)
. /etc/profile.d/slurm.sh

##### VARIABLES
# the name to give to the job - do not use spaces
JOB1_NAME="file-downloads-stat-copy-data"
JOB2_NAME="file-downloads-stat-analysis"
# memory limit
MEMORY_LIMIT=12G
#email notification
JOB_EMAIL="pride-report@ebi.ac.uk"

LOGS_SOURCE_ROOT=$LOGS_SOURCE_ROOT
LOGS_DESTINATION_ROOT=$LOGS_DESTINATION_ROOT

##### FUNCTIONS
printUsage() {
    echo "Description: a pipeline to calculate number of file downloads"
    echo "$ ./runFileDownloadStats.sh"
    echo ""
    echo "Usage: ./runFileDownloadStats.sh"
    echo "     Example: ./runFileDownloadStats.sh"
}

if [ -z "$LOGS_SOURCE_ROOT" ]; then
    echo "LOGS_SOURCE_ROOT is not set!"
    exit 1
fi
if [ -z "$LOGS_DESTINATION_ROOT" ]; then
    echo "LOGS_DESTINATION_ROOT is not set!"
    exit 1
fi


DATE=$(date +"%Y%m%d%H%M")

##### Change directory to where the script locate
cd ${0%/*}

#### RUN the first job on the cluster #####
JOB_ID=$(sbatch --time=00:07:00  \
    --mem="${MEMORY_LIMIT}" \
    -p datamover \
    --mail-type=ALL \
    --mail-user="${JOB_EMAIL}" \
    --job-name="${JOB1_NAME}" <<EOF
#!/bin/bash
rsync -ah --progress --stats \
    "${LOGS_SOURCE_ROOT}/fasp-aspera/public" \
    "${LOGS_SOURCE_ROOT}/ftp/public" \
    "${LOGS_SOURCE_ROOT}/gridftp-globus/public" \
    "${LOGS_SOURCE_ROOT}/http/public" \
    "${LOGS_DESTINATION_ROOT}/"
EOF
)


# Extract the job ID from the sbatch output (e.g., "Submitted batch job 12345") This removes everything up to the first space, leaving only the job ID
JOB_ID=${JOB_ID##* }

${CONDA_INIT}
conda activate file_download_stat
PROFILE="ebislurm"

#### RUN it on the cluster #####
sbatch -t 7-0 \
     --mem=${MEMORY_LIMIT} \
     -p standard \
     --mail-type=ALL \
     --mail-user=${JOB_EMAIL} \
     --job-name=${JOB2_NAME} \
     --dependency=afterok:${JOB_ID} \
     ./scripts/run_stat.sh ${PROFILE}