## A. Requirements

🔹 System Requirements
   
   1. Operating System:
      - Linux (preferred)
      - macOS (works with Conda)
      - Windows (with WSL2)

   2. Software Dependencies:
      - conda (Miniconda or Anaconda)
      - mamba (installed via Conda, if missing)
      - make (for running the Makefile)
      - bash (since Makefile runs Bash scripts)
      - git (recommended for version control)

## B. 📖 How to Install & Run from Local Environment

**1️⃣ Clone the Repository**
   Clone the repository and go to the location where you installed the pipeline. 
   ```bash
   git clone git@github.com:PRIDE-Archive/file-download-stat.git
   cd file-download-stat
   ```

**2️⃣ Run the Installer**
   Run the following command to set up the environment and install dependencies:
   ```bash
   make setup      # Set up required variables
   make install    # Run all checks and install dependencies
   ```
   This will:
   
   - Set up environment variables
   - Install Conda if missing
   - Install the required Conda environment (file_download_stat)
   - Verify necessary paths

**3️⃣ Copy Log Files**
   Copy log files from the EBI server to your local machine. 
   ```bash
    scp -r <username>@<server>:<path_to_log_files> <LOGS_DESTINATION_ROOT>
   ```
**4️⃣ Run the Pipeline**
   After the installation is complete, run the pipeline with:
   ```bash
   scripts/run_stat_local.sh local 
   ```

## C. 📖How to Install & Run from EBI Infrastructure

**1️⃣ Fork the Repository**  
   Fork the [file-download-stat](https://github.com/PRIDE-Archive/file-download-stat) repository to your GitHub account.

**2️⃣ Set Up EBI GitLab Repo**  
   Set up a repository in GitLab to mirror the repository you forked from GitHub.

**3️⃣ Customize `params.config` File**  
   In the `params` folder, customize or add your `<resource_name>-<environment>-params.config` file to configure your pipeline parameters.

**4️⃣ Set Up CI/CD Pipeline and Variables**  
   Set up the CI/CD pipeline in GitLab. Be sure to configure any necessary environment variables that are required for your pipeline.

**5️⃣ Deploy the Pipeline from GitLab**  
   Deploy the pipeline from GitLab, making sure that the pipeline correctly uses your configuration.

**6️⃣ Run `make install` Command**  
   Go to the location where you installed the pipeline and run the following command to set up the environment and install dependencies:
   ``` bash
   make setup      # Set up required variables
   make install    # Run all checks and install dependencies
   ```
   
**7️⃣ Run the Pipeline**
   After the installation is complete, run the pipeline with:
   ``` bash
   ./run_download_stat.sh
   ```

**8️⃣ Optionally, Set Up Seqera Environment**
   If needed, you can optionally set up the Seqera environment to monitor the running pipelines. Please contact us for more information.
