CREATE TABLE [utility].[JsonSchemaDataMapping] (
    [SourceSystemTypeName] NVARCHAR (128) NOT NULL,
    [SourceDataTypeName]   NVARCHAR (128) NOT NULL,
    [ExportDataTypeName]   NVARCHAR (128) NOT NULL
);
GO

CREATE CLUSTERED INDEX [CIX_SourceSystemSourceType]
    ON [utility].[JsonSchemaDataMapping]([SourceSystemTypeName] ASC, [SourceDataTypeName] ASC);
GO

