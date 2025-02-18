# file-download-stat

[![Nextflow](https://img.shields.io/badge/nextflow%20DSL2-%E2%89%A522.10.6-23aa62.svg)](https://www.nextflow.io/)
[![run with conda](https://img.shields.io/badge/run%20with-conda-3EB049?labelColor=000000&logo=anaconda)](https://docs.conda.io/en/latest/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


[//]: # ([![run with singularity]&#40;https://img.shields.io/badge/run%20with-singularity-1d355c.svg?labelColor=000000&#41;]&#40;https://sylabs.io/docs/&#41;)

## Introduction

A nextflow pipeline get the statistics around file downloads from the log files saved in the EBI infrastructure.
This helps to understand the usage of the files and the projects, and helps to make decisions.


## Usage

To Create the Environment:

`conda env create -f environment.yml`

To Activate conda :

`conda activate file_download_stat`

## Debugging

Check Nextflow:

`nextflow -version`
