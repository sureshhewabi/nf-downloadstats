/*
========================================================================================
                 Validation Workflow for PRIDE Data
========================================================================================
 @#### Authors
 Suresh Hewapathirana <sureshhewabi@gmail.com>
----------------------------------------------------------------------------------------
*/

/*
 * Define the default parameters
 */
params.root_dir=''
params.output_file='parsed_data.parquet'
params.log_file=''


log.info """\
 ===================================================
  F I L E    D O W N L O A D    S T A T I S T I C S
 ===================================================


FOR DEVELOPERS USE

Nextflow location   : ${params.nextflow_location}
Data Base Dir       : ${params.data_base_dir}
LaunchDir           : $workflow.launchDir
projectDir          : $workflow.projectDir
workDir             : $workflow.workDir
SessionId           : $workflow.sessionId
RunName             : $workflow.runName
NextFlow version    : $nextflow.version
Date                : ${new java.util.Date()}

 """

 /*
  * Read tsv.gz log files and genarate individual parquet files
  */
 process read_logs_and_generate_parquets {

    input:
    val root_dir
    val output_file

    output:
    file "${output_file}"

    script:
    """
     python3 $workflow.projectDir/filedownloadstat/main.py generate-parquet \
     -r $root_dir \
     -o $output_file
    """
 }

 process get_stat_from_parquet_files {
    input:
    path parquet_file

    output:
    file "stat.tsv"

     script:
     """
     python3 $workflow.projectDir/filedownloadstat/main.py stat \
     -f $parquet_file \
     -o "stat.tsv" \
     """
 }

workflow {
    read_logs_and_generate_parquets(params.root_dir, params.output_file) | get_stat_from_parquet_files
}