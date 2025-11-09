CREATE TABLE [meta].[SourceObjects] (
    [ID]                 INT            IDENTITY (1, 1) NOT NULL,
    [SourceConnectionID] INT            NOT NULL,
    [SchemaName]         NVARCHAR (200) NOT NULL,
    [ObjectName]         NVARCHAR (200) NOT NULL,
    [ExtractSQLFilter]   NVARCHAR (MAX) NOT NULL,
    [IncrementalFlag]    BIT            NOT NULL,
    [KeyColumnFlag]      BIT            NOT NULL,
    [IncludeFlag]        BIT            NOT NULL
);
GO

ALTER TABLE [meta].[SourceObjects]
    ADD CONSTRAINT [DF_SourceObjects_ExtractSQLFilter] DEFAULT ('') FOR [ExtractSQLFilter];
GO

ALTER TABLE [meta].[SourceObjects]
    ADD CONSTRAINT [DF_SourceObjects_KeyColumnFlag] DEFAULT ((0)) FOR [KeyColumnFlag];
GO

ALTER TABLE [meta].[SourceObjects]
    ADD CONSTRAINT [DF_SourceObjects_ExcludeFlag] DEFAULT ((1)) FOR [IncludeFlag];
GO

ALTER TABLE [meta].[SourceObjects]
    ADD CONSTRAINT [DF_SourceObjects_IncrementalFlag] DEFAULT ((0)) FOR [IncrementalFlag];
GO

ALTER TABLE [meta].[SourceObjects]
    ADD CONSTRAINT [AK_SourceObjects_SourceConnectionID_SchemaName_ObjectName] UNIQUE NONCLUSTERED ([SourceConnectionID] ASC, [SchemaName] ASC, [ObjectName] ASC);
GO

ALTER TABLE [meta].[SourceObjects]
    ADD CONSTRAINT [PK_SourceObjects] PRIMARY KEY CLUSTERED ([ID] ASC);
GO

