This wiki provides documentation for twoday data & AI's Fabric Lakehouse framework "AquaVilla". The wiki is targeted back-end developers and provides:
- Introduction
- Getting Started
- Advanced Topics

# Introduction
Welcome to twoday data & ai’s best practice module for Lakehouse based data platform solutions using the [Spark engine](https://spark.apache.org/) in Microsoft Fabric. Also known as AquaVilla.

This module is part of a larger set of modules that together constitute twoday data & ai’s Data Platform Best Practice Framework (BPF).

This module has a sister module, the best practice module for Lakehouse based data platform solutions using Azure Databricks (Lighthouse). The Databricks module and the Fabric module share naming standards, architectural principles, code, etc. They separate only in areas where Databricks and Fabric are different with respect to features that the two modules have an advantage of utilizing. Example: Databricks has a concept of “Delta Live Tables”, that Fabric doesn’t. Hence the two best practice modules differ because the Databricks based module utilizes “Delta Live Tables”, whereas the Fabric based module doesn’t.

## Purpose
The purpose of this module is to

- Accelerate development of Lakehouse data platforms 
- Ensure quality and robustness 
- Ease maintenance and operations

## Design principles
twoday data & ai’s Fabric Lakehouse Best Practice module is built on:

- Metadata driven approach : A separate Azure SQL Database contains meta data used to control e.g. ingestion of data, processing of Fabric notebooks, etc.
- Naming standards : The names of Fabric items such as Lakehouse items are standardized across this framework and the databricks based framework - more naming standards are applied "inside" the framework.
- Architectural choices based on years of experience with data platform solution development
- Deep expert insight into the underlying technology stack, in this case Microsoft Fabric.
- To use Fabric items when ever possible. Only use other Azure services if a Fabric version isn't available or is not mature enough.


## Architectural Overview 

![Image alt text](FabricLakehouseArchitecture.png "Fabric Lakehouse Architecture")

The illustration above shows several of the design-decisions of the best practice framework with Fabric.

Data is transformed by Fabric notebooks, which in some cases call other notebooks and libraries containing twoday data & ai’s framework functionality.

## Layers

The framework is designed based on well-defined data layers, where data moves from layer to layer, undergoing certain transformations along the way following the principles of the [medallion architecture](https://www.databricks.com/glossary/medallion-architecture) from Databrics.

The framework follows the proposed three layers from the medallion architecture, but also opens up to use a more fine grained transformation journey if this is requried.

Below is a more detailed description of each layer and its intended purpose. 

### Landing

|  |  |
| --------------- | --------------- |
| **Purpose** | Landing zone for data from any external source system. Can host both batch and streaming data. |
| **What's inside** | Unmodified data from the source systems. |
| **Data format(s) inside layer** | Files in any format including structured, unstructured, semi-structured. Native format with no schema needed. REST sources in JSON, databases in parquet format etc. |
| **Processing of data entering layer** | Metadata driven pull process. Data may also be pushed from source systems. No transformation. |
| **Organization of data inside layer** | In the Files section of the Fabric lakehouse item. If data is ordered into source entities, such as source tables, they will be organized into folders for each table with hierarchies on year, date, etc. See example in [Ingest](01-ingest.md)|

### Raw
An optional layer that can be introduced if needed.

|  |  |
| --------------- | --------------- |
| **Purpose** | To build up a history layer from which any potential rerun of subsequent processing towards following layers can be performed. Can host both batch and streaming data.  |\
| **What's inside** | Unmodified data from the source systems. Event based payloads are deserialized.|
| **Data format(s) inside layer** | Delta. Schema is applied. Schema may be inferred. Hence data can be easily queried and analyzed|
| **Processing of data entering layer** | Metadata driven process for easy onboarding of new data into the Lakehouse. Conversion to delta table format.|
| **Organization of data inside layer** | Data is written into the Table section of the lakehouse item for each source entity.|

### Base

|  |  |
| --------------- | --------------- |
| **Purpose** | To build up a functional history layer, ensure data usability, reliability, and quality in an early stage. Can host both batch and streaming data.|
| **What's inside** | Deduplicated and quality enforced data from the source systems with human readable column names.|
| **Data format(s) inside layer** | Delta. Schema is applied.|
| **Processing of data entering layer** | [Metadata driven process](02-landing-to-base.md) for easy onboarding of new data. Possible transformations: <br /> - Raw transformations if not applied in a raw layer <br /> - Deduplication <br /> - Renaming of columns <br /> - Quality enforcement <br /> - Handling null values <br /> - Exploding of nested fields|
| **Organization of data inside layer** | Data is written into the Table section of the lakehouse item for each source entity.|

### Enriched
An optional layer that can be introduced if needed.

|  |  |
| --------------- | --------------- |
| **Purpose** | To match, merge and conform data into an "enterprise view" of business entities and concepts. For instance, employee data from different sources could be unioned into a single employee entity. Can host both batch and streaming data.|
| **What's inside** | Data represented as business entities.|
| **Data format(s) inside layer** | Delta. Schema is applied.|
| **Processing of data entering layer** | Single table process. Possible transformations:<br /> - Base transformations if not applied in a base layer <br /> - Joining /union tables <br /> - Consolidation logic <br /> - Adding cross references|
| **Organization of data inside layer** | Data is written into the Table section of the lakehouse item for each source entity.|

### Curated

|  |  |
| --------------- | --------------- |
| **Purpose** | To make data consumption ready for specific purposes. Can host both batch and streaming data.|
| **What's inside** | Consumption ready data such as dimension and fact tables for dimensional models.|
| **Data format(s) inside layer** | Delta. Schema is applied.|
| **Processing of data entering layer** | Single table process ready to be served for consumers. Possible transformations: <br /> - Enriched transformations if not applied in an enriched layer <br /> - Business logic <br /> - Aggregation <br /> - Adding surrogate keys <br /> - Relating dimensions and facts|
| **Organization of data inside layer** | Data is written into the Table section of the lakehouse item for business areas / projects.|


## Additional components

- Azure Data Factory is used for [ingest](01-ingest.md) and [highlevel orchestration](06-orchestrate.md) as Fabric still lacks important Data Pipeline features.
- A separate small-size Azure SQL Database stores metadata which is being used in several areas of the framework.
- Secrets are managed in Azure Key Vault and used by the metadata-driven data processing notebooks.
- Azure DevOps is used to store all code, etc. under source control and for development collaboration and for solution deployment to test and production.
- During development Fabric Git Integration to Azure DevOps is applied for source code control.
- Semantic models are developed and maintained with Tabular Editor for efficiency.