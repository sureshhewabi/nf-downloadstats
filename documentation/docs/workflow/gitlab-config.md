# GitLab CI/CD

This document outlines the **environment variables** that must be provided in **GitLab CI/CD** for the pipeline to function correctly. These variables should be configured in the **GitLab repository settings** under **Settings > CI/CD > Variables**.

---

## **Required Environment Variables**

| **Variable Name**                        | **Description** |
|------------------------------------------|---------------|
| `API_ENDPOINT_FILE_DOWNLOADS_PER_FILE`  | API endpoint for updating file-level download statistics. |
| `API_ENDPOINT_FILE_DOWNLOADS_PER_PROJECT` | API endpoint for updating project-level download statistics. |
| `API_ENDPOINT_HEADER`                    | Authorization header for API requests. |
| `CONDA_INIT`                              | Path to the Conda initialization script. |
| `DATA_ROOT_DIR`                           | Root directory where data is stored. |
| `DEPLOY_PATH`                             | Path where the pipeline artifacts or results will be deployed. |
| `DEPLOY_SERVER`                           | The server address where deployment occurs. |
| `LOG_FOLDER`                              | Directory where logs should be stored. |
| `NEXTFLOW`                                | Path to the Nextflow executable. |
| `PIPELINE_BASE_DIR`                       | Base directory of the pipeline execution. |
| `SERVER_USER`                             | Username for the deployment server. |
| `SSH_KEY`                                 | SSH private key for accessing the deployment server. |
| `SYMLINK`                                 | Path to create symbolic links for processed data. |
| `WORK_PATH`                               | Working directory for pipeline execution. |

---

## **Setting Up Variables in GitLab CI/CD**

To configure these variables:  
1. Navigate to your **GitLab project**.  
2. Go to **Settings > CI/CD**.  
3. Expand the **Variables** section.  
4. Click **Add Variable** and enter the name, value, and choose the appropriate **scope** and **protection**.  
5. Click **Save variables**.

!!! note ".gitlab-ci.yml"

    Ensure that your `.gitlab-ci.yml` file references these variables correctly

---

## **Security Recommendations**
- Protect sensitive variables (SSH_KEY, API_ENDPOINT_HEADER) by marking them as protected and masked.
- Avoid exposing secrets in logs by setting Mask Variable when adding them to GitLab CI/CD.
- Use SSH keys securely by ensuring SSH_KEY is configured correctly for authentication.

## **Troubleshooting**
If any variable is missing or incorrectly set, the pipeline may fail. Check the following:

- Ensure all required variables are defined in Settings > CI/CD > Variables.
- Use echo $VARIABLE_NAME in the pipeline script to debug missing variables.
- Check GitLab CI/CD logs for any environment variable errors.