

## **Parameters**
| Parameter | Description |
|-----------|-------------|
| `params.root_dir` | The root directory containing log files |
| `params.output_file` | Designated output filename for the Parquet dataset |
| `params.log_file` | Path to the primary log file |
| `params.api_endpoint_file_download_per_project` | API endpoint for project-level file download statistics |
| `params.protocols` | Protocols considered in the processing pipeline |

Additional parameters relevant for debugging and report generation:
- `params.resource_identifiers`
- `params.completeness`
- `params.public_private`
- `params.report_template`
- `params.log_file_batch_size`
- `params.resource_base_url`
- `params.report_copy_filepath`
- `params.skipped_years`
- `params.accession_pattern`
- `params.chunk_size`
- `params.disable_db_update`
- `params.api_endpoint_file_downloads_per_file`

---

## **Debugging and Error Handling**
- The workflow captures session metadata and logs critical information at runtime.
- Intermediate outputs are generated to facilitate validation and troubleshooting.
- Fault tolerance is enhanced via retry mechanisms using `error_retry_max` and `error_retry_medium` labels.

---

## **Additional Considerations**
- The workflow is optimized for high-throughput log processing and large-scale statistical analysis.
- Database updates can be toggled using the `params.disable_db_update` flag.
- Input log files may be in compressed (`.gz`) or uncompressed (`.tsv`) format.

---

## **Execution Instructions**
To initiate the workflow, execute:
```bash
nextflow run main.nf --root_dir /path/to/logs --output_file parsed_data.parquet
```

For debugging and performance monitoring, enable logging:
```bash
nextflow run main.nf -with-report report.html -with-trace trace.txt
```

