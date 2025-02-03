# Nextflow SLURM Configuration Documentation

This document outlines the SLURM resource configurations and process-specific settings used in **Nextflow**. It explains CPU, memory, time allocations, and error handling strategies for different process types.

---

## **Global Process Configuration**

The following global resource allocation settings are applied to all Nextflow processes by default:

| **Parameter**      | **Description**                                        | **Computation**              |
|--------------------|--------------------------------------------------------|------------------------------|
| `cpus`            | Number of CPU cores assigned to a task                 | `2 * task.attempt`           |
| `memory`          | Amount of memory allocated                             | `8 GB * task.attempt`        |
| `time`            | Maximum execution time per task                        | `4 h * task.attempt`         |
| `errorStrategy`   | Defines how the workflow handles errors                | Retries on exit status `130-145` and `104`, otherwise finishes |
| `maxRetries`      | Maximum number of retry attempts for a task            | `3`                          |
| `maxErrors`       | Maximum number of errors allowed before failing        | `-1` (Unlimited errors)      |

---

## **Process-Specific Resource Allocations**

Each **process label** defines custom resource requirements based on workload demands. These labels are commonly used in **nf-core DSL2 modules** and can be reused in local modules.

### **1. Single-Core Processes** (`process_single`)

| **Resource** | **Allocation** |
|-------------|---------------|
| `cpus`      | `1`           |
| `memory`    | `6 GB * task.attempt` |
| `time`      | `4 h * task.attempt`  |

---

### **2. Very Low Resource Processes** (`process_very_low`)

| **Resource** | **Allocation** |
|-------------|---------------|
| `cpus`      | `1 * task.attempt` |
| `memory`    | `1 GB * task.attempt` |
| `time`      | `1 h * task.attempt`  |

---

### **3. Low Resource Processes** (`process_low`)

| **Resource** | **Allocation** |
|-------------|---------------|
| `cpus`      | `4 * task.attempt`  |
| `memory`    | `12 GB * task.attempt` |
| `time`      | `6 h * task.attempt`  |

---

### **4. Medium Resource Processes** (`process_medium`)

| **Resource** | **Allocation** |
|-------------|---------------|
| `cpus`      | `8 * task.attempt`  |
| `memory`    | `36 GB * task.attempt` |
| `time`      | `8 h * task.attempt`  |

---

### **5. High Resource Processes** (`process_high`)

| **Resource** | **Allocation** |
|-------------|---------------|
| `cpus`      | `12 * task.attempt` |
| `memory`    | `72 GB * task.attempt` |
| `time`      | `16 h * task.attempt` |

---

### **6. Long-Running Processes** (`process_long`)

| **Resource** | **Allocation** |
|-------------|---------------|
| `time`      | `20 h * task.attempt` |

---

### **7. High-Memory Processes** (`process_high_memory`)

| **Resource** | **Allocation** |
|-------------|---------------|
| `memory`    | `200 GB * task.attempt` |

---

## **Error Handling Strategies**

Some processes have customized error handling strategies:

### **Ignore Errors** (`error_ignore`)

| **Strategy**      | **Action** |
|------------------|-----------|
| `errorStrategy`  | `ignore`  |

---

### **Retry Errors** (`error_retry`)

| **Strategy**      | **Action** |
|------------------|-----------|
| `errorStrategy`  | `retry`   |
| `maxRetries`     | `3`       |

---

## **Global Parameter Defaults**

The following **default limits** are defined for resource usage but can be overwritten when running the workflow.

| **Parameter**   | **Default Value** |
|----------------|----------------|
| `max_memory`   | `128 GB`       |
| `max_cpus`     | `16`           |
| `max_time`     | `240 h`        |

---

## **Example Configuration File**

```nextflow
params {
    max_memory = 128.GB
    max_cpus = 16
    max_time = 240.h
}
```