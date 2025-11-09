
CREATE PROCEDURE [utility].[UpdateSourceObjectIncrementalNavisionSetup]

AS

DROP TABLE IF EXISTS #Data

  SELECT 
	 [SourceObjects].[ID] AS SourceObjectID
	,SourceConnectionNavisionSetup.ID AS SourceConnectionNavisionSetupID
  INTO
	#Data
  FROM [meta].[SourceObjects]
  INNER JOIN
	meta.SourceConnections
		ON SourceConnections.ID = [SourceObjects].SourceConnectionID
		AND SourceConnections.NavisionFlag = 1
  INNER JOIN
	meta.SourceConnectionNavisionSetup
		ON SourceConnectionNavisionSetup.SourceConnectionID = SourceConnections.ID
  LEFT JOIN
	meta.SourceObjectIncrementalNavisionSetup
		ON SourceObjectIncrementalNavisionSetup.SourceObjectID = [SourceObjects].[ID]
		AND SourceObjectIncrementalNavisionSetup.SourceConnectionNavisionSetupID = SourceConnectionNavisionSetup.ID
  WHERE
	[SourceObjects].IncrementalFlag = 1
	AND SourceObjectIncrementalNavisionSetup.ID IS NULL
	AND SourceConnectionNavisionSetup.IncludeFlag = 1

  INSERT INTO meta.SourceObjectIncrementalNavisionSetup WITH (TABLOCK)
  ( SourceObjectID
   ,SourceConnectionNavisionSetupID)

   SELECT
    SourceObjectID
   ,SourceConnectionNavisionSetupID
   FROM #Data
GO

