# Orchestration


## Notebook

When utilizing notebooks for orchestration, there are two modes available for running subsequent notebooks: synchronous and asynchronous. The AquaVilla package provides examples of both. It's on the task list to investigate reports concerning race conditions when running asynchronously, so be aware of this.

Use a notebook to orchestrate other notebooks with synchronous run the use of mssparkutils.notebook.run

    runOutput = mssparkutils.notebook.run("Load_DimCalendar", 300)
    print(runOutput)

    runOutput = mssparkutils.notebook.run("Load_DimProduct", 300)
    print(runOutput)


Use a notebook to orchestrate other notebooks with asynchronous run the use of mssparkutils.notebook.runMultiple

Here is an example to execute three dimensions:

    dimensionRuns = {
        "activities" : [
            {
                "name": "AquaVilla_LoadDimCalendar", 
                "path": "AquaVilla_LoadDimCalendar", 
                "timeoutPerCellInSeconds": 300, 
                "retry": 1,
                "retryIntervalInSeconds": 10,
                "args": {"recreate": recreate}
            },
            {
                "name": "AquaVilla_LoadDimCustomer", 
                "path": "AquaVilla_LoadDimCustomer", 
                "timeoutPerCellInSeconds": 300, 
                "retry": 1,
                "retryIntervalInSeconds": 10,
                "args": {"recreate": recreate}
            },
            {
                "name": "AquaVilla_LoadDimEmployee", 
                "path": "AquaVilla_LoadDimEmployee", 
                "timeoutPerCellInSeconds": 300, 
                "retry": 1,
                "retryIntervalInSeconds": 10,
                "args": {"recreate": recreate}
            }
        ]
    }

    output = mssparkutils.notebook.runMultiple(dimensionRuns)

## Pipeline in ADF

A common pattern is to handle the overall high-level orchestration from Azure Data Factory to create a dependency between the Ingest (pipelines) and the Prepare (notebooks).

Notebooks in Fabric can be executed with use of a "Web" activity that calls the "Job Scheduler - Run On Demand Item Job" REST API.

Example:

`POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances?jobType=RunNotebook`

More information:
 - https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-public-api#run-a-notebook-on-demand
 - https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job
