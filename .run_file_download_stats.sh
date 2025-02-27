#!/usr/bin/env bash

# Load environment (and make the slrum command available)
. /etc/profile.d/slurm.sh

##### VARIABLES
# the name to give to the job - do not use spaces
JOB1_NAME="file-downloads-stat-copy-data"
JOB2_NAME="file-downloads-stat-analysis"
# memory limit
MEMORY_LIMIT=7G
#email notification
JOB_EMAIL="pride-report@ebi.ac.uk"

##### FUNCTIONS
printUsage() {
    echo "Description: a pipeline to calculate number of file downloads"
    echo "$ ./runFileDownloadStats.sh"
    echo ""
    echo "Usage: ./runFileDownloadStats.sh"
    echo "     Example: ./runFileDownloadStats.sh"
}

DATE=$(date +"%Y%m%d%H%M")

##### Change directory to where the script locate
cd ${0%/*}

JOB_ID=$(sbatch -t 1-0 \
      --mem="${MEMORY_LIMIT}" \
      -p datamover \
      --mail-type=ALL \
      --mail-user="${JOB_EMAIL}" \
      --job-name="${JOB1_NAME}" <<EOF | awk '/Submitted batch job/ {print $NF}'
#!/bin/bash
rsync -ah --progress --stats "${LOGS_SOURCE_ROOT}/fasp-aspera/public/" "${LOGS_DESTINATION_ROOT}/fasp-aspera/public/"
rsync -ah --progress --stats "${LOGS_SOURCE_ROOT}/ftp/public/" "${LOGS_DESTINATION_ROOT}/ftp/public/"
rsync -ah --progress --stats "${LOGS_SOURCE_ROOT}/gridftp-globus/public/" "${LOGS_DESTINATION_ROOT}/gridftp-globus/public/"
rsync -ah --progress --stats "${LOGS_SOURCE_ROOT}/http/public/" "${LOGS_DESTINATION_ROOT}/http/public/"
EOF
)

JOB_ID=$(echo "$JOB_ID" | awk '{print $NF}')

echo "Captured sbatch output: $JOB_ID"

if [[ -z "$JOB_ID" ]]; then
    echo "ERROR: Job ID is empty! sbatch might have failed."
    exit 1
fi

echo "${JOB1_NAME} job ID: ${JOB_ID}"

${CONDA_INIT}
conda activate file_download_stat
RESOURCE=${RESOURCE}
PROFILE=${PROFILE}

#### RUN it on the cluster #####
sbatch -t 3-0 \
      --mem=${MEMORY_LIMIT} \
      -p standard \
      --mail-type=ALL \
      --mail-user=${JOB_EMAIL} \
      --job-name=${JOB2_NAME} \
      --dependency=afterok:${JOB_ID} \
      ./scripts/run_stat.sh ${RESOURCE} ${PROFILE}