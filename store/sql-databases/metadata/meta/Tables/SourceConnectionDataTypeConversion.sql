CREATE TABLE [meta].[SourceConnectionDataTypeConversion] (
    [ID]                  INT           IDENTITY (1, 1) NOT NULL,
    [SourceConnectionID]  INT           NOT NULL,
    [ConvertFromDataType] NVARCHAR (50) NOT NULL,
    [ConvertToDataType]   NVARCHAR (50) NOT NULL
);
GO

ALTER TABLE [meta].[SourceConnectionDataTypeConversion]
    ADD CONSTRAINT [PK_SourceDataTypeConversion] PRIMARY KEY CLUSTERED ([ID] ASC);
GO

