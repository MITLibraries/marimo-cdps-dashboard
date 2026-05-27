# cdps-dashboard
A [Marimo notebook](https://marimo.io/) that uses S3 inventory data to display statistics about content in MIT Libraries' preservation storage. The analysis logic depends on Archivematica package and MIT Libraries file naming conventions. After launching the notebook, see the "About this notebook" section for more information.


## Developing 
As this dashboard relies on S3 Inventory data, authenticate with `Dev1` credentials before editing.

The recommended approach for developing a Marimo notebook is to use the Marimo GUI editor:

```shell
make edit-notebook
```

This [Confluence page](https://mitlibraries.atlassian.net/wiki/x/AYA3IgE) describes the update workflow for this notebook. All updates are done in coordination with and fully reviewed by the [DataEng team](https://github.com/orgs/MITLibraries/teams/dataeng).

### Testing
To run tests:

```shell
make test
```

### Linting
To run linting:

```shell
make lint
```

## Environment Variables

### Required
```shell
S3_INVENTORY_LOCATIONS=# A comma-delimited list of S3 URIs containing S3 Inventory symlink.txt files.
```

### Optional
```shell
# add optional env vars here...
```

## Running
Often, notebooks are [served as an "app"](https://docs.marimo.io/guides/apps/).  This is the default mode for [marimo-launcher](https://github.com/MITLibraries/marimo-launcher).

```shell
uv run marimo run --sandbox --headless --no-token notebook.py
```

Access to an AWS-hosted notebook is managed through secrets stored in LastPass. For details on accessing this hosted notebook, visit this [Confluence page](https://mitlibraries.atlassian.net/wiki/x/AQCNFwE).
