# Serving Data

## Semantic Models

The default semantic models created with each Lakehouse should **not** be used.

Instead, custom semantic models should be built based on data from the **Curated** layer. These models can be created in either Direct Lake or Import mode, depending on the use case. It is recommended to deploy these custom models to a separate "Serve" workspace and store the corresponding code in a "Serve" folder within the DevOps repository.

The preferred tool for creating semantic models is **Tabular Editor**, and the [XMLA endpoint read-write](https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-connect-tools#enable-xmla-read-write) feature must be enabled in the capacity settings under workloads.

### Import Mode

In most cases, the classic Import mode remains the recommended choice for building semantic models. Import mode provides better performance for report queries by caching the data.

When using Import mode:

- Build semantic models on top of views exposed through the SQL analytics endpoint.

#### TODO:

- Outline specific scenarios where Import Mode offers a distinct advantage.

### Direct Lake Mode

**Direct Lake** is a flagship feature of Fabric. It eliminates the need for data duplication by enabling direct access to the data stored in the Lakehouse. However, users should be aware of potential trade-offs:

- Query response times may be longer compared to Import Mode.
- Certain scenarios may trigger a “fallback to Direct Query mode,” which can significantly degrade query performance.

To create a Direct Lake semantic model, follow [this guide](https://blog.tabulareditor.com/2023/09/26/fabric-direct-lake-with-tabular-editor-part-2-creation/) using Tabular Editor 3.

#### TODO:

- Describe scenarios where Direct Lake Mode provides a clear benefit.
- Explain how Semantic Link can be utilized to warm up the cache for improved performance.

## SQL Analytics Endpoint

Each Lakehouse includes a **read-only SQL endpoint** that allows you to run Transact-SQL statements to query, filter, aggregate, and explore data in Lakehouse tables. Every Delta table from the Lakehouse is represented as a table in this endpoint.

The SQL analytics endpoint can be used to:

- Expose data in a relational format to Semantic Models, Business Analysts, and/or applications.
- Validate and test data in the **Curated** layer.
- Optionally, provide access to specific users to validate and test data in the **Base** layer.

### TODO:

- Describe how to configure access to the SQL analytics endpoint.
- Provide instructions for setting up role-based security for the SQL endpoint.
