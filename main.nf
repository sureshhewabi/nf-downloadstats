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
params.root_dir='/Users/hewapathirana/Desktop/transfer_logs_privacy_ready'
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

process get_log_files {
    // Define inputs and outputs
    input:
    val root_dir

    output:
    path "file_list.txt"

    script:
    """
    python3 ${workflow.projectDir}/filedownloadstat/main.py get_log_files \
        --root_dir $root_dir \
        --output "file_list.txt"
    """
}

process process_log_file {
    input:
    val file_path  // Each file object from the channel

    output:
    path "*.parquet",optional: true  // Output files with unique names

    script:
    """
    # Extract a unique identifier from the log file name
    filename=\$(basename ${file_path} .log.tsv.gz)
    python3 ${workflow.projectDir}/filedownloadstat/main.py process_log_file \
        -f ${file_path} \
        -o "\${filename}.parquet"
    """
}

process get_stat_from_parquet_files {
    input:
    path parquet_file

    output:
    path "*.stat.tsv"  // Output files with unique names

    script:
    """
    # Extract a unique identifier from the Parquet file name
    filename=\$(basename ${parquet_file} .parquet)
    python3 $workflow.projectDir/filedownloadstat/main.py stat \
        -f ${parquet_file} \
        -o "\${filename}.stat.tsv"
    """
}

process combine_stat_files {
    input:
    path stat_files

    output:
    path "summary_stat.tsv"

    script:
    """
    # Combine all stat.tsv files into one summary file without headers
    awk 'NR == 1 || FNR > 1' ${stat_files.join(' ')} > summary_stat.tsv
    """
}



workflow {
    // Step 1: Gather file names
    def root_dir = params.root_dir
    get_log_files(root_dir)
        .splitText()                // Split file_list.txt into individual lines
        .map { it.trim() }          // Trim any extra whitespace or newlines
        .set { file_path }          // Save the channel

    // Step 2: Process each log file and generate Parquet files
    def parquet_files = process_log_file(file_path)

    // Step 3: Process the generated Parquet files to calculate statistics
    def stat_files = get_stat_from_parquet_files(parquet_files)

    // Step 4: Combine all stat.tsv files into a single summary file
    stat_files
        .collect()  // Collect all `.stat.tsv` files into a single list
        .set { all_stat_files }

    combine_stat_files(all_stat_files)
}