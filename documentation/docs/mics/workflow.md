
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