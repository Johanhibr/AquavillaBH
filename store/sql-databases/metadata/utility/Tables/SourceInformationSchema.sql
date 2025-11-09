CREATE TABLE [utility].[SourceInformationSchema] (
    [SourceConnectionID]     INT            NULL,
    [SourceConnectionName]   NVARCHAR (128) NULL,
    [SourceSystemTypeName]   NVARCHAR (128) NULL,
    [TableCatalogName]       NVARCHAR (128) NULL,
    [SchemaName]             NVARCHAR (128) NULL,
    [TableName]              NVARCHAR (128) NULL,
    [ColumnName]             NVARCHAR (128) NULL,
    [OrdinalPositionNumber]  INT            NULL,
    [NullableName]           NVARCHAR (128) NULL,
    [DataTypeName]           NVARCHAR (128) NULL,
    [MaximumLenghtNumber]    NVARCHAR (38)  NULL,
    [NumericPrecisionNumber] INT            NULL,
    [NumericScaleNumber]     INT            NULL,
    [KeySequenceNumber]      INT            NULL,
    [NavisionFlag]           BIT            NULL
);
GO

