
## **Workflow Architecture**
### **1. Retrieve Log Files (`get_log_files`)**
- Identifies and compiles a list of relevant log files from the root directory.
- Output: `file_list.txt`

### **2. Compute Log File Statistics (`run_log_file_stat`)**
- Performs statistical analysis on extracted log files.
- Output: `log_file_statistics.html`

### **3. Process Log Files (`process_log_file`)**
- Extracts and transforms log data into Parquet format for structured analysis.
- Output: `*.parquet` files.

### **4. Merge Parquet Datasets (`merge_parquet_files`)**
- Aggregates individual Parquet datasets into a singular consolidated dataset.
- Output: `output_parquet`

### **5. Analyze Merged Dataset (`analyze_parquet_files`)**
- Conducts comprehensive statistical analysis on the merged dataset.
- Outputs:
  - `project_level_download_counts.json`
  - `file_level_download_counts.json`
  - `project_level_yearly_download_counts.json`
  - `project_level_top_download_counts.json`
  - `all_data.json`

### **6. Generate Download Statistics Report (`run_file_download_stat`)**
- Produces a visual analytical report based on the processed dataset.
- Output: `file_download_stat.html`

### **7. Update Project-Level Download Metrics (`update_project_download_counts`)**
- Uploads the aggregated project-level download statistics to a designated database.
- Output: `upload_response_file_downloads_per_project.txt`

### **8. Update File-Level Download Metrics (`update_file_level_download_counts`)**
- Segments large JSON datasets into smaller subsets for database ingestion.
- Output: Server response files confirming successful uploads.

---

## **Execution Flow**
1. **Retrieve log files** → `file_list.txt`
2. **Analyze log file statistics** → `log_file_statistics.html`
3. **Transform log files** → Parquet dataset
4. **Merge datasets** → `output_parquet`
5. **Analyze aggregated dataset** → JSON statistics reports
6. **Generate statistical visualization** → `file_download_stat.html`
7. **Update database (if enabled)**
   - Project-level download metrics
   - File-level download metrics (batch processing enabled)

---


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
