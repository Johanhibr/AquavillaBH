CREATE TABLE [meta].[SourceConnections] (
    [ID]             INT            IDENTITY (1, 1) NOT NULL,
    [ConnectionType] NVARCHAR (100) NOT NULL,
    [Name]           NVARCHAR (250) NOT NULL,
    [NavisionFlag]   BIT            NOT NULL,
    [IncludeFlag]    BIT            NOT NULL
);
GO

ALTER TABLE [meta].[SourceConnections]
    ADD CONSTRAINT [PK_SourceConnections] PRIMARY KEY CLUSTERED ([ID] ASC);
GO

ALTER TABLE [meta].[SourceConnections]
    ADD CONSTRAINT [AK_SourceConnections_Name] UNIQUE NONCLUSTERED ([Name] ASC);
GO

ALTER TABLE [meta].[SourceConnections]
    ADD CONSTRAINT [DF_SourceConnections_ExcludeFlag] DEFAULT ((1)) FOR [IncludeFlag];
GO

ALTER TABLE [meta].[SourceConnections]
    ADD CONSTRAINT [DF_SourceConnections_Name] DEFAULT ('') FOR [Name];
GO

ALTER TABLE [meta].[SourceConnections]
    ADD CONSTRAINT [DF_SourceConnections_ConnectionType_1] DEFAULT ('') FOR [ConnectionType];
GO

ALTER TABLE [meta].[SourceConnections]
    ADD CONSTRAINT [DF_SourceConnections_ConnectionType] DEFAULT ((0)) FOR [NavisionFlag];
GO

