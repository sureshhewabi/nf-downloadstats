# Configuration Parameters

## **Log File Parsing**

- **`protocols`**  
  A list of protocols supported for data transfer.
  - **Values:**
    - `fasp-aspera`: Aspera FASP protocol.
    - `gridftp-globus`: GridFTP with Globus.
    - `http`: HTTP protocol.
    - `ftp`: FTP protocol.

- **`public_private`**  
  A list of access types for the dataset.
  - **Values:**
    - `public`: Data that is publicly available.
    - `private`: Data that is privately available.

- **`resource_identifiers`**  
  A list of identifiers that define valid resource locations.
  - **Values:**
    - `/pride/data/archive`: Path to PRIDE archive data.
    - `/pride-archive`: Path to an alternate PRIDE archive.

- **`accession_pattern`**  
  A regular expression pattern to validate the accession IDs of the dataset.
  - **Default:** `'^PXD\\d{6}$'`
  - **Explanation:** This pattern is designed to match PRIDE accession IDs (e.g., `PXD000123`).

- **`completeness`**  
  A list of possible data completion statuses.
  - **Values:**
    - `complete`: Indicates that the file is complete.
    - `incomplete`: Indicates that the file has not downloaded successfully.
  
- **`log_file_batch_size`**  
  The number of rows to process in each batch when reading log files.
  - **Default:** `1000`
  - **Explanation:** Controls how many lines are processed in each batch when parsing large log files.


---

## **Statistics Reports**

- **`report_template`**  
  The filename of the HTML template to be used for generating reports.
  - **Default:** `"pride_report.html"`
  - **Explanation:** This template is used for generating a formatted report of the PRIDE data. 
However, create your own template for your resource by customizing this template and specify it in the configuration file.

- **`skipped_years`**  
  A list of years for which reports will be skipped.
  - **Values:**  
    - Example: `[2020, 2025]`  
    - These years will be excluded from the report generation.

- **`resource_base_url`**  
  The base URL for accessing the resources (e.g., project details).
  - **Default:** `'https://www.ebi.ac.uk/pride/archive/projects/'`
  - **Explanation:** This URL is used to link to the resource projects in the PRIDE archive. This is used to link back accessions mentioned in the report to your original dataset in your resource.

- **`report_copy_filepath`**  
  The file path where the report copy will be saved after processing.
  - **Default:** `/your/path/Desktop`
  - **Explanation:** The generated report will be saved to this location on the local system.

---

## **Push to a Database**

- **`disable_db_update`**  
  A flag to enable or disable updates to the database(e.g., MongoDB).
  - **Values:**
    - `true`: Disable database updates.
    - `false`: Enable database updates.

- **`chunk_size`**  
  The size of data chunks to be handled during processing.
  - **Default:** `100000`
  - **Explanation:** Determines how much data to process at a time, based on this information, file level statistics will be saved into several JSON files to reduce the payload size when you upload them into your database(Ex: MongoDB)

---

## Example Configuration File

```yaml
############# Log File Parsing #############

protocols:
  - fasp-aspera
  - gridftp-globus
  - http
  - ftp
public_private:
  - public
resource_identifiers:
  - /pride/data/archive
  - /pride-archive
accession_pattern: '^PXD\\d{6}$'
completeness:
  - complete
log_file_batch_size: 1000

############# Statistics Reports #############

report_template: "pride_report.html"
skipped_years:
  - 2020
  - 2025
resource_base_url: 'https://www.ebi.ac.uk/pride/archive/projects/'
report_copy_filepath: /your/machine/Desktop

############# Push to a Database(e.g MongoDB) #############

disable_db_update: true
chunk_size: 100000
```
