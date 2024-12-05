/*
========================================================================================
                 FILE Download Statistics Workflow
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
params.api_endpoint_file_download_per_project=''
params.api_endpoint_header=''
params.protocols=''


log.info """\
 ===================================================
  F I L E    D O W N L O A D    S T A T I S T I C S
 ===================================================


FOR DEVELOPERS USE

SessionId           : $workflow.sessionId
LaunchDir           : $workflow.launchDir
projectDir          : $workflow.projectDir
workDir             : $workflow.workDir
RunName             : $workflow.runName
NextFlow version    : $nextflow.version
Nextflow location   : ${params.nextflow_location}
Date                : ${new java.util.Date()}
Protocols           : ${params.protocols}
Protocols           : ${params.resource_identifiers}
Protocols           : ${params.completeness}

 """

process get_log_files {

    label 'data_mover'

    input:
    val root_dir

    output:
    path "file_list.txt"

    script:
    """
    python3 ${workflow.projectDir}/filedownloadstat/main.py get_log_files \
        --root_dir $root_dir \
        --output "file_list.txt" \
        --protocols '${params.protocols.join(" ")}'
    """
}

process process_log_file {

    label 'process_very_low'
    label 'data_mover'
    label 'error_retry'


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
        -o "\${filename}.parquet" \
        -r '${params.resource_identifiers.join(" ")}' \
        -c '${params.completeness.join(" ")}' \
        > process_log_file.log 2>&1
    """
}

process analyze_parquet_files {

    label 'process_low'

    input:
    val all_parquet_files  // A comma-separated string of file paths

    output:
    path "summed_accession_counts.json"

    script:
    """
    # Write the file paths to a temporary file, because otherwise Argument list(file list) will be too long
    echo "${all_parquet_files.join('\n')}" > all_parquet_files_list.txt

    python3 ${workflow.projectDir}/filedownloadstat/main.py get_file_counts \
        --input_dir all_parquet_files_list.txt \
        --output_summed summed_accession_counts.json
    """
}

process uploadJsonFile {

    input:
    path jsonFile // The JSON file to upload

    output:
    path "upload_response.txt" // Capture the response from the server

    script:
    """
    curl --location '${params.api_endpoint_file_download_per_project}' \
    --header '${params.api_endpoint_header}' \
    --form 'files=@\"${jsonFile}\"' > upload_response.txt
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
        .set { jsonFile }           // Set JSON file output to a new channel

    // Step 4: Upload the JSON file
//     uploadJsonFile(jsonFile) // TODO: Only testing purpose

}