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

process get_log_files {

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

process analyze_parquet_files {

    input:
    val all_parquet_files  // A comma-separated string of file paths

    output:
    path "grouped_file_counts.tsv"
    path "summed_accession_counts.tsv"

    script:
    """
    # Write the file paths to a temporary file, because otherwise Argument list(file list) will be too long
    echo "${all_parquet_files.join('\n')}" > all_parquet_files_list.txt

    python3 ${workflow.projectDir}/filedownloadstat/main.py get_file_counts \
        --input_dir all_parquet_files_list.txt \
        --output_grouped grouped_file_counts.tsv \
        --output_summed summed_accession_counts.tsv
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
    def all_parquet_files = process_log_file(file_path)

    // Collect all parquet files into a single channel for analysis
    all_parquet_files
        .collect()                  // Collect all parquet files into a single list
        .set { parquet_file_list }  // Save the collected files as a new channel

    // Step 3: Analyze Parquet files
    analyze_parquet_files(parquet_file_list)

}