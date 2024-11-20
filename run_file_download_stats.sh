#!/usr/bin/env bash

# Load environment (and make the slrum command available)
. /etc/profile.d/slurm.sh

##### VARIABLES
# the name to give to the job - do not use spaces
JOB_NAME="file-downloads-statistics"
# memory limit
MEMORY_LIMIT=8G
#email notification
JOB_EMAIL="pride-report@ebi.ac.uk"


##### FUNCTIONS
printUsage() {
    echo "Description: a pipeline to calculate number of file downloads"
    echo "$ ./scripts/runFileDownloadStats.sh"
    echo ""
    echo "Usage: ./runFileDownloadStats.sh"
    echo "     Example: ./runFileDownloadStats.sh"
}

DATE=$(date +"%Y%m%d%H%M")

##### Change directory to where the script locate
cd ${0%/*}

conda activate nextflow
PROFILE="conda"

#### RUN it on the cluster #####
sbatch -t 7-0 \
     --mem=${MEMORY_LIMIT} \
     -p datamover \
     --mail-type=ALL \
     --mail-user=${JOB_EMAIL} \
     --job-name=${JOB_NAME} \
     ${PIPELINE_BASE_DIR}/scripts/run_stat.sh ${PROFILE}