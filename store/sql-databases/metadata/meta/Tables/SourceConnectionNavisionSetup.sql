CREATE TABLE [meta].[SourceConnectionNavisionSetup] (
    [ID]                 INT            IDENTITY (1, 1) NOT NULL,
    [SourceConnectionID] INT            NOT NULL,
    [CompanyName]        NVARCHAR (200) NOT NULL,
    [IncludeFlag]        BIT            NOT NULL
);
GO

ALTER TABLE [meta].[SourceConnectionNavisionSetup]
    ADD CONSTRAINT [DF_SourceConnectionNavisionSetup_SourceConnectionID] DEFAULT ((0)) FOR [SourceConnectionID];
GO

ALTER TABLE [meta].[SourceConnectionNavisionSetup]
    ADD CONSTRAINT [DF_SourceObjectNavisionSetup_ExtractFlag] DEFAULT ((1)) FOR [IncludeFlag];
GO

ALTER TABLE [meta].[SourceConnectionNavisionSetup]
    ADD CONSTRAINT [DF_SourceObjectNavisionSetup_CompanyName] DEFAULT ('') FOR [CompanyName];
GO

