# Contributing Guidelines

Many thanks for taking an interest in improving this nextflow pipeline!

## Contribution workflow

If you'd like to give a code contribution:

1. Check that there isn't already an issue about your idea in the [GitHub Issues](https://github.com/PRIDE-Archive/file-download-stat/issues) to avoid duplicating work. If there isn't one already, please create one so that others know you're working on this.
2. [Fork](https://github.com/PRIDE-Archive/file-download-stat) to your GitHub account
3. Make the necessary changes / additions within your forked repository following [Pipeline conventions](#pipeline-contribution-conventions)
4. Submit a Pull Request against the `main` branch and wait for the code to be reviewed and merged

## Pipeline contribution conventions

### Adding a new step

If you wish to contribute a new step, please use the following coding standards:

1. Define the corresponding input channel into your new process from the expected previous process channel
2. Write the process block (see below).
3. Define the output channel if needed (see below).
4. Add any new parameters to `<resource_name>-<environment>-params.config`.
5. Add any new variables to GitLab Variables(if any) and update `.github/workflow/deploy.yml` accordingly.
6. Perform local tests to check that the new code works as expected.
7. If applicable, add a new test command in `.github/workflow/ci.yml`.