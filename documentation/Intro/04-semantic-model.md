# Semantic model(s)

The default semantic models that are created with each Lakehouse should not be used!

Based of data from the Curated layer, one ore more **custom** semantic models are created in either Direct Lake or Import mode.

It's recommended to deploy the semantic model(s) to a separate workspace and also store the code in a separate DevOps repo.

Tool of choice is Tabular Editor and [XMLA endpoint read-write](https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-connect-tools#enable-xmla-read-write) should be enabled under workloads in the capacity settings.

## Direct Lake mode

Direct Lake is positioned as a “killer feature” of Fabric. However users may experience longer query response times compared to Import Mode. Also a number of situations will make Fabric “fallback to direct query mode”, impacting the report query performance significantly.


### Create a Direct Lake semantic model

How to get started creating a basic semantic model in Direct Lake mode

 - Navigate to the Curated Lakehouse
 - Click on "New semantic model"
   - Give it a name
   - Choose a different workspace where it should be saved
   - Select the tables to be used in the model
   - Click "Confirm"
 - Close / exit the editor that appears
 - Connect to the newly created "online" model with Tabular Editor via the XMLA endpoint
   - Make a manual change - add a measure or create a relationship.
   - Save offline as .bim format or "save to folder".
 - Deploy the offline model back on top of the model in the workspace via the XMLA endpoint

The whole process can also be done alone with Tabular Editor 3 with [this guide](https://blog.tabulareditor.com/2023/09/26/fabric-direct-lake-with-tabular-editor-part-2-creation/).

## Import mode

In most cases it's recommended to continue and use the classic import mode, when building the models.
