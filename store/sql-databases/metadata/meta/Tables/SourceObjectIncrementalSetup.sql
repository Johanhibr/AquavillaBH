CREATE TABLE [meta].[SourceObjectIncrementalSetup] (
    [ID]                               INT            IDENTITY (1, 1) NOT NULL,
    [SourceObjectID]                   INT            NOT NULL,
    [IncrementalValueColumnDefinition] NVARCHAR (500) NOT NULL,
    [IsDateFlag]                       BIT            NOT NULL,
    [LastValueLoaded]                  NVARCHAR (500) NOT NULL,
    [RollingWindowDays]                INT            NULL
);
GO

ALTER TABLE [meta].[SourceObjectIncrementalSetup]
    ADD CONSTRAINT [DF_SourceObjectIncrementalSetup_IsDateFlag] DEFAULT ((0)) FOR [IsDateFlag];
GO

ALTER TABLE [meta].[SourceObjectIncrementalSetup]
    ADD CONSTRAINT [DF_SourceObjectIncrementalSetup_IncrementalValueColumnName] DEFAULT ('') FOR [IncrementalValueColumnDefinition];
GO

ALTER TABLE [meta].[SourceObjectIncrementalSetup]
    ADD CONSTRAINT [DF_SourceObjectIncrementalSetup_RollingWindowNumber] DEFAULT ((0)) FOR [RollingWindowDays];
GO

ALTER TABLE [meta].[SourceObjectIncrementalSetup]
    ADD CONSTRAINT [AK_SourceObjectIncrementalSetup_ID] UNIQUE NONCLUSTERED ([ID] ASC);
GO

