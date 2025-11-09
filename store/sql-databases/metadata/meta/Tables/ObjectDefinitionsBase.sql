CREATE TABLE [meta].[ObjectDefinitionsBase] (
    [ID]                       INT            IDENTITY (1, 1) NOT NULL,
    [SourcePath]               NVARCHAR (250) NOT NULL,
    [SourceType]               NVARCHAR (50)  NOT NULL,
    [SourceReadBehavior]       NVARCHAR (50)  NOT NULL,
    [ReadOptions]              VARCHAR (4000) NULL,
    [DestinationTable]         NVARCHAR (200) NOT NULL,
    [DestinationWriteBehavior] NVARCHAR (50)  NOT NULL,
    [Keys]                     NVARCHAR (100) NOT NULL,
    [ProjectedColumns]         NVARCHAR (500) NULL,
    [ModifyDateColumn]         NVARCHAR (250) NULL,
    [Translations]             VARCHAR (4000) NULL,
    [PartitionBy]              NVARCHAR (200) NULL,
    [CustomTransformation]     VARCHAR (200) NULL,
    [RecreateFlag]             BIT            NOT NULL,
    [IncludeFlag]              BIT            NOT NULL
);
GO

