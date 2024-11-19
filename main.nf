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
params.output_file=''
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
//     output:
//     file "${output_file}" into output_parquet

    script:
    """
     python3 $workflow.projectDir/filedownloadstat/main.py generate-parquet \
     -r $root_dir \
     -o $output_file
    """
 }

// Workflow definition
workflow {
    // Pass the parameters to the process
    output_parquet = read_logs_and_generate_parquets(params.root_dir, params.output_file)
}