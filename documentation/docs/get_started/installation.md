### A. Run in your Local Environment

1. **Download the project**<br>
    Download/clone the project from [file-download-stat repository](https://github.com/PRIDE-Archive/file-download-stat)
2. **Run `make install` Command**  
   Go to the location where you installed the pipeline and run the following command to set up the environment and install dependencies:
   ```bash
   make setup      # Set up required variables
   make install    # Run all checks and install dependencies
   ```
3. Copy log files from the EBI server to your local machine. 
4. **Run the Pipeline**
   After the installation is complete, run the pipeline with:
   ```bash
   scripts/run_stat.sh local 
   ```

### B. Run in EBI Infrastructure

1. **Fork the Repository**  
   Fork the [file-download-stat repository](https://github.com/PRIDE-Archive/file-download-stat) to your GitHub account.

2. **Set Up EBI GitLab Repo**  
   Set up a repository in GitLab to mirror the repository you forked from GitHub.

3. **Customize `params.config` File**  
   In the `params` folder, customize or add your `<resource_name>-<environment>-params.config` file to configure your pipeline parameters.

4. **Set Up CI/CD Pipeline and Variables**  
   Set up the CI/CD pipeline in GitLab. Be sure to configure any necessary environment variables that are required for your pipeline.

5. **Deploy the Pipeline from GitLab**  
   Deploy the pipeline from GitLab, making sure that the pipeline correctly uses your configuration.

6. **Run `make install` Command**  
   Go to the location where you installed the pipeline and run the following command to set up the environment and install dependencies:
   ``` bash
   make setup      # Set up required variables
   make install    # Run all checks and install dependencies
   ```
   
7. **Run the Pipeline**
   After the installation is complete, run the pipeline with:
   ``` bash
   ./run_download_stat.sh
   ```

8. **Optionally, Set Up Seqera Environment**
   If needed, you can optionally set up the Seqera environment to monitor the running pipelines. Please contact us for more information.
