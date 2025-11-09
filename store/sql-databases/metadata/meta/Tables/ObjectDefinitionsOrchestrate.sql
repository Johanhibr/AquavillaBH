CREATE TABLE [meta].[ObjectDefinitionsOrchestrate] (
    [ID]                      INT            IDENTITY (1, 1) NOT NULL,
    [NotebookName]            NVARCHAR (250) NOT NULL,
    [NotebookPath]            NVARCHAR (250) NOT NULL,
    [TimeoutPerCellInSeconds] INT            DEFAULT ((90)) NOT NULL,
    [RetryCount]              INT            DEFAULT ((1)) NOT NULL,
    [RetryIntervalInSeconds]  INT            DEFAULT ((10)) NOT NULL,
    [ArgumentDefinition]      NVARCHAR (250) DEFAULT ('') NOT NULL,
    [DependencyDefinition]    NVARCHAR (250) DEFAULT ('') NOT NULL,
    [Triggers]                NVARCHAR (250) NOT NULL,
    [IncludeFlag]             BIT            DEFAULT ((1)) NOT NULL
);
GO

