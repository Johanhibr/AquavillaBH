CREATE TABLE [meta].[SourceObjectIncrementalNavisionSetup] (
    [ID]                              INT            IDENTITY (1, 1) NOT NULL,
    [SourceObjectID]                  INT            NOT NULL,
    [SourceConnectionNavisionSetupID] INT            NOT NULL,
    [LastValueLoaded]                 NVARCHAR (500) NOT NULL
);
GO

ALTER TABLE [meta].[SourceObjectIncrementalNavisionSetup]
    ADD CONSTRAINT [DF_SourceObjectIncrementalNavisionSetup_LastValueLoaded] DEFAULT ('0') FOR [LastValueLoaded];
GO

ALTER TABLE [meta].[SourceObjectIncrementalNavisionSetup]
    ADD CONSTRAINT [AK_SourceObjectIncrementalNavisionSetup_ID] UNIQUE NONCLUSTERED ([ID] ASC);
GO

