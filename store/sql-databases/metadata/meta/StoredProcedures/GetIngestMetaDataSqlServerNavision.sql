
CREATE PROCEDURE [meta].[GetIngestMetaDataSqlServerNavision] 

	@JobIsIncremental BIT,
	@ConnectionName NVARCHAR(50),
	@SchemaName NVARCHAR(50),
	@TableName NVARCHAR(50)

AS

--Prepare source datasets to improve performance

DROP TABLE IF EXISTS #SourceInformationSchemaDefinitions SELECT * INTO #SourceInformationSchemaDefinitions FROM utility.SourceInformationSchemaDefinitions WHERE ConnectionName = @ConnectionName
DROP TABLE IF EXISTS #SourceObjectDefinitions SELECT * INTO #SourceObjectDefinitions FROM meta.SourceObjectDefinitions WHERE ConnectionName = @ConnectionName

--Create Output
DROP TABLE IF EXISTS #NavisionData

	SELECT 
		 ConnectionType						= SourceObjectDefinitions.ConnectionType
		,ConnectionName						= SourceObjectDefinitions.ConnectionName
		,SourceConnectionID					= SourceObjectDefinitions.SourceConnectionID
		,SchemaName							= SourceObjectDefinitions.SchemaName
		,TableName                          = SourceObjectDefinitions.TableName
		,TableNameAlias						= SourceInformationSchemaDefinitions.AliasTableName
		,SourceObjectID						= SourceObjectDefinitions.SourceObjectID 
		,IncrementalFlag					= SourceObjectDefinitions.IncrementalFlag
		,LoadType							= IIF(SourceObjectDefinitions.IncrementalFlag = 1,IIF(@JobIsIncremental = 1,'Delta','Full'),'Full')
		,NavisionFlag						= SourceObjectDefinitions.NavisionFlag
		,ExtractSQLFilter					= SourceObjectDefinitions.ExtractSQLFilter
		,KeyColumns							= SourceObjectDefinitions.KeyColumns
		,SqlKeys							= 'SELECT ' + STRING_AGG(IIF(SourceInformationSchemaDefinitions.KeySequenceNumber IS NULL,NULL, CAST(IIF(SourceConnectionDataTypeConversion.ConvertToDataType IS NULL,QUOTENAME(SourceInformationSchemaDefinitions.SourceColumnName),'CAST(' + QUOTENAME(SourceInformationSchemaDefinitions.SourceColumnName) + ' AS ' + SourceConnectionDataTypeConversion.ConvertToDataType + ')') + ' AS ' + QUOTENAME(SourceInformationSchemaDefinitions.AliasColumnName) AS NVARCHAR(MAX))),', ')	
		,SqlSelect							= 'SELECT ' + 
													STRING_AGG(CAST(IIF(SourceConnectionDataTypeConversion.ConvertToDataType IS NULL,QUOTENAME(SourceInformationSchemaDefinitions.SourceColumnName),'CAST(' + QUOTENAME(SourceInformationSchemaDefinitions.SourceColumnName) + ' AS ' + SourceConnectionDataTypeConversion.ConvertToDataType + ')') + ' AS ' + QUOTENAME(SourceInformationSchemaDefinitions.AliasColumnName) AS NVARCHAR(MAX)),', ')			
	INTO 
		#NavisionData
	FROM 
		#SourceInformationSchemaDefinitions AS SourceInformationSchemaDefinitions
	INNER JOIN
		#SourceObjectDefinitions AS SourceObjectDefinitions
			ON  SourceObjectDefinitions.SourceObjectID = SourceInformationSchemaDefinitions.SourceObjectID 
	LEFT JOIN
		meta.SourceConnectionDataTypeConversion
			ON SourceConnectionDataTypeConversion.SourceConnectionID = SourceObjectDefinitions.SourceConnectionID
			AND SourceConnectionDataTypeConversion.ConvertFromDataType = SourceInformationSchemaDefinitions.DataTypeName  
	WHERE
			SourceObjectDefinitions.NavisionFlag = 1
		AND IIF(NULLIF(@SchemaName,'') IS NULL,'1',@SchemaName) = IIF(NULLIF(@SchemaName,'') IS NULL,'1',SourceObjectDefinitions.SchemaName)
		AND IIF(NULLIF(@TableName,'') IS NULL,'1',@TableName) = IIF(NULLIF(@TableName,'') IS NULL,'1',SourceObjectDefinitions.TableName)
	GROUP BY
		 SourceObjectDefinitions.ConnectionType
		,SourceObjectDefinitions.ConnectionName
		,SourceObjectDefinitions.SchemaName
		,SourceObjectDefinitions.ExtractSQLFilter
		,SourceObjectDefinitions.SourceObjectID
		,SourceObjectDefinitions.SourceConnectionID
		,SourceObjectDefinitions.IncrementalFlag
		,SourceObjectDefinitions.NavisionFlag
		,SourceObjectDefinitions.KeyColumns
		,SourceObjectDefinitions.TableName
		,SourceInformationSchemaDefinitions.AliasTableName



	SELECT
		 ConnectionType
		,ConnectionName
		,SchemaName
		,TableName
		,TableNameAlias
		,SourceObjectID				= #NavisionData.SourceObjectID
		,IncrementalFlag
		,LoadType
		,KeyColumns				
		,SqlKeys					= STRING_AGG(SqlKeys + 		
													',''' + SourceConnectionNavisionSetup.CompanyName + ''' AS CompanyName' + 
													' FROM ' + CAST(SchemaName  AS NVARCHAR(MAX)) + '.' + CAST(QUOTENAME(SourceConnectionNavisionSetup.CompanyName + '$' + TableName) AS NVARCHAR(MAX)) + 
													' WHERE 1 = 1' + IIF(ExtractSQLFilter = '','',' AND ' + ExtractSQLFilter) , ' UNION ALL ')
		,SqlNewWatermark			= IIF(IncrementalFlag = 0, 'SELECT 1 AS NewWatermark',
													STRING_AGG('SELECT 
															''' + CAST(SourceObjectIncrementalNavisionSetup.SourceConnectionNavisionSetupID AS NVARCHAR(50)) + ''' AS SourceConnectionNavisionSetupID
														,''' + SourceConnectionNavisionSetup.CompanyName + ''' AS CompanyName
														,  MAX(CONVERT(BIGINT, [timestamp])) AS NewWaterMark 
													FROM ' + CAST(SchemaName  AS NVARCHAR(MAX)) + '.' + CAST(QUOTENAME(SourceConnectionNavisionSetup.CompanyName + '$' + TableName) AS NVARCHAR(MAX)), ' UNION ALL '))

		,SqlQuery					= STRING_AGG(SqlSelect + 
													', ''' + SourceConnectionNavisionSetup.CompanyName + ''' AS CompanyName 
													FROM ' + CAST(SchemaName  AS NVARCHAR(MAX)) + '.' + CAST(QUOTENAME(SourceConnectionNavisionSetup.CompanyName + '$' + TableName) AS NVARCHAR(MAX)) + 
													' WHERE 1 = 1' + IIF(ExtractSQLFilter = '','',' AND ' + ExtractSQLFilter) + IIF(IncrementalFlag = 1 AND @JobIsIncremental = 1, ' AND CONVERT(BIGINT, [timestamp]) > ''' + ISNULL(SourceObjectIncrementalNavisionSetup.LastValueLoaded,'0') + '''','') , ' UNION ALL ')
												
	FROM 
		#NavisionData
	LEFT JOIN
		meta.SourceConnectionNavisionSetup
			ON SourceConnectionNavisionSetup.SourceConnectionID = #NavisionData.SourceConnectionID
	LEFT JOIN
		meta.SourceObjectIncrementalNavisionSetup
			ON SourceObjectIncrementalNavisionSetup.SourceConnectionNavisionSetupID = SourceConnectionNavisionSetup.ID
			AND SourceObjectIncrementalNavisionSetup.SourceObjectID = #NavisionData.SourceObjectID
	WHERE
		SourceConnectionNavisionSetup.IncludeFlag = 1
	GROUP BY 
		 ConnectionType
		,ConnectionName
		,SchemaName
		,TableName
		,#NavisionData.SourceObjectID
		,IncrementalFlag
		,NavisionFlag
		,SqlKeys
		,SqlSelect
		,KeyColumns
		,TableName
		,TableNameAlias
		,LoadType
GO

