#!/usr/bin/env bash

# Load environment (and make the slrum command available)
. /etc/profile.d/slurm.sh

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="File Downloads Statistics"
# memory limit
MEMORY_LIMIT=8G
#email notification
JOB_EMAIL="pride-report@ebi.ac.uk"


##### FUNCTIONS
printUsage() {
    echo "Description: a pipeline to calculate number of file downlaods"
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
     -o /dev/null \
     -e /dev/null \
     ${PIPELINE_BASE_DIR}/scripts/runStat.sh ${PROFILE}