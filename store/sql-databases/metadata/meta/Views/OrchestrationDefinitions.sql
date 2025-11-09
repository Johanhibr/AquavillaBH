Create view meta.OrchestrationDefinitions as
SELECT  
      [NotebookName] AS name
      ,[NotebookPath] AS path
      ,[TimeoutPerCellInSeconds] as timeoutPerCellInSeconds
      ,[RetryCount] as retry
      ,[RetryIntervalInSeconds] as retryIntervalInSeconds
      ,[ArgumentDefinition] as args
      ,[DependencyDefinition] as dependencies
  FROM [meta].[ObjectDefinitionsOrchestrate]
  WHERE 
	IncludeFlag = 1
GO

