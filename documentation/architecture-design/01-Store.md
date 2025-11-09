# Store Data

The framework employs a structured approach with clearly defined data layers. Data progresses through these layers, undergoing specific transformations, adhering to the principles of the [medallion architecture](https://www.databricks.com/glossary/medallion-architecture) by Databricks.

This framework provides a baseline architecture based on the three core layers (Landing, Base, and Curated) of the medallion model. Additionally, it allows for more granular transformation steps by introducing optional layers, such as Raw and Enriched, if required.

![Data Layers](DataLayers.png "Data Layers")

Detailed descriptions of each layer and their intended roles are provided below.

## Landing

| Attribute | Description |
| --------------- | --------------- |
| **Purpose** | Acts as the initial zone for data ingestion from any external source system, supporting both batch and streaming data. |
| **Content** | Unaltered data directly sourced from external systems. |
| **Data Formats** | Supports all file formats, including structured, semi-structured, and unstructured formats. Examples include JSON for REST sources and Parquet for databases. No schema is required. |
| **Processing** | Involves a metadata-driven pull process, though data can also be pushed from source systems. No transformations are applied. |
| **Organization** | Stored in the Files section of the Fabric lakehouse. Data is typically organized by source entities (e.g., source tables), with folder hierarchies based on attributes like year and date. See [Ingest](Ingest) for examples. |

## Raw (Optional)

| Attribute | Description |
| --------------- | --------------- |
| **Purpose** | Provides a historical archive for data, enabling reruns of downstream processing. Supports both batch and streaming data. |
| **Content** | Unmodified source data, with event-based payloads deserialized. |
| **Data Formats** | Delta format with applied or inferred schemas, making data queryable and analyzable. |
| **Processing** | Involves a metadata-driven process for easy onboarding and conversion to Delta table format. |
| **Organization** | Data is stored in the Table section of the lakehouse, organized by source entities. |

## Base

| Attribute | Description |
| --------------- | --------------- |
| **Purpose** | Serves as a functional historical layer, ensuring data usability, reliability, and quality at an early stage. Supports both batch and streaming data. |
| **Content** | Deduplicated, quality-controlled data with human-readable column names. |
| **Data Formats** | Delta format with applied schemas. |
| **Processing** | [Metadata-driven](Prepare) onboarding with possible transformations, including:<br /> - Raw layer transformations (if not performed earlier) <br /> - Deduplication <br /> - Column renaming <br /> - Data quality enforcement <br /> - Null value handling <br /> - Exploding nested fields |
| **Organization** | Data is stored in the Table section of the lakehouse, organized by source entities. |

## Enriched (Optional)

| Attribute | Description |
| --------------- | --------------- |
| **Purpose** | Aligns and consolidates data into an "enterprise view" of business entities, such as merging employee data from multiple sources into a unified employee table. Supports both batch and streaming data. |
| **Content** | Business entities and concepts. |
| **Data Formats** | Delta format with applied schemas. |
| **Processing** | Involves single-table transformations, such as:<br /> - Base layer transformations (if not applied earlier) <br /> - Joining or unioning tables <br /> - Consolidation logic <br /> - Adding cross-references |
| **Organization** | Data is stored in the Table section of the lakehouse, organized by business entities. |

## Curated

| Attribute | Description |
| --------------- | --------------- |
| **Purpose** | Prepares data for direct consumption, tailored to specific business needs. Supports both batch and streaming data. |
| **Content** | Ready-to-use data, such as dimension and fact tables for dimensional modeling. |
| **Data Formats** | Delta format with applied schemas. |
| **Processing** | Involves transformations aimed at consumption readiness, including:<br /> - Enriched layer transformations (if not performed earlier) <br /> - Applying business logic <br /> - Aggregations <br /> - Generating surrogate keys <br /> - Relating dimensions and facts |
| **Organization** | Data is stored in the Table section of the lakehouse, organized by business areas or projects. |