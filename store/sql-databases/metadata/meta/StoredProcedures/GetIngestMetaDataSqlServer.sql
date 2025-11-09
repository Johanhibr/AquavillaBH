


CREATE PROCEDURE [meta].[GetIngestMetaDataSqlServer] 

	@JobIsIncremental BIT,
	@ConnectionName NVARCHAR(50),
	@SchemaName NVARCHAR(50),
	@TableName NVARCHAR(50)

AS

--Prepare source datasets to improve performance

DROP TABLE IF EXISTS #SourceInformationSchemaDefinitions SELECT * INTO #SourceInformationSchemaDefinitions FROM utility.SourceInformationSchemaDefinitions WHERE ConnectionName = @ConnectionName
DROP TABLE IF EXISTS #SourceObjectDefinitions SELECT * INTO #SourceObjectDefinitions FROM meta.SourceObjectDefinitions WHERE ConnectionName = @ConnectionName

--Create Output

	SELECT 
		 ConnectionType						= SourceObjectDefinitions.ConnectionType
		,ConnectionName						= SourceObjectDefinitions.ConnectionName
		,SourceConnectionID					= SourceObjectDefinitions.SourceConnectionID
		,SchemaName							= SourceObjectDefinitions.SchemaName
		,TableName							= SourceObjectDefinitions.TableName
		,TableNameAlias						= SourceObjectDefinitions.TableName
		,SourceObjectID						= SourceObjectDefinitions.SourceObjectID 
		,IncrementalFlag					= SourceObjectDefinitions.IncrementalFlag
		,LoadType							= IIF(SourceObjectDefinitions.IncrementalFlag = 1,IIF(@JobIsIncremental = 1,'Delta','Full'),'Full')
		,ExtractSQLFilter					= SourceObjectDefinitions.ExtractSQLFilter
		,KeyColumns							= SourceObjectDefinitions.KeyColumns
		,SqlKeys							= 'SELECT ' + STRING_AGG(IIF(SourceInformationSchemaDefinitions.KeySequenceNumber IS NULL,NULL, CAST(IIF(SourceConnectionDataTypeConversion.ConvertToDataType IS NULL,QUOTENAME(SourceInformationSchemaDefinitions.SourceColumnName),'CAST(' + QUOTENAME(SourceInformationSchemaDefinitions.SourceColumnName) + ' AS ' + SourceConnectionDataTypeConversion.ConvertToDataType + ')') + ' AS ' + QUOTENAME(SourceInformationSchemaDefinitions.AliasColumnName) AS NVARCHAR(MAX))),', ')	+
											  ' FROM '  + SourceObjectDefinitions.SchemaName + '.' + QUOTENAME(SourceObjectDefinitions.TableName) +
											  ' WHERE 1 = 1' 
													   + IIF(SourceObjectDefinitions.ExtractSQLFilter = '','',' AND ' + SourceObjectDefinitions.ExtractSQLFilter) 
		,SqlNewWatermark					= CASE 
												WHEN IncrementalFlag = 0 THEN 'SELECT 1 AS NewWatermark'
												ELSE 'SELECT ISNULL(CONVERT(BIGINT, ' 
																+ IIF (SourceObjectDefinitions.IsDateFlag = 1,'FORMAT(MAX(' + SourceObjectDefinitions.IncrementalValueColumnDefinition + '), ''yyyyMMddHHmmss'')),','MAX(' + SourceObjectDefinitions.IncrementalValueColumnDefinition + ')),') 
																+ IIF (SourceObjectDefinitions.IsDateFlag = 1,'''19000101000000''','''0''') + ') AS NewWatermark FROM [' + SourceObjectDefinitions.SchemaName + '].[' + SourceObjectDefinitions.TableName + ']'
											  END
		,SqlQuery							= 'SELECT ' + 
														STRING_AGG(CAST(IIF(SourceConnectionDataTypeConversion.ConvertToDataType IS NULL,QUOTENAME(SourceInformationSchemaDefinitions.SourceColumnName),'CAST(' + QUOTENAME(SourceInformationSchemaDefinitions.SourceColumnName) + ' AS ' + SourceConnectionDataTypeConversion.ConvertToDataType + ')') + ' AS ' + QUOTENAME(SourceInformationSchemaDefinitions.AliasColumnName) AS NVARCHAR(MAX)),', ')	+	
											  ' FROM '  + SourceObjectDefinitions.SchemaName + '.' + QUOTENAME(SourceObjectDefinitions.TableName) +
											  ' WHERE 1 = 1' 
													   + IIF(SourceObjectDefinitions.ExtractSQLFilter = '','',' AND ' + SourceObjectDefinitions.ExtractSQLFilter) 
													   + IIF(SourceObjectDefinitions.IncrementalFlag = 0 OR @JobIsIncremental = 0,'',' AND ' + SourceObjectDefinitions.IncrementalValueColumnDefinition 
													   + ' > ' + 
															CASE
																WHEN SourceObjectDefinitions.IsDateFlag = 1 THEN 'CONVERT(DATETIME, STUFF(STUFF(STUFF('''+ SourceObjectDefinitions.LastValueLoaded +''', 9, 0, '' ''), 12, 0, '':''), 15, 0, '':''))'
																ELSE '' + SourceObjectDefinitions.LastValueLoaded + ''
															END )

											  	

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
			SourceObjectDefinitions.ConnectionName	= @ConnectionName
		AND SourceObjectDefinitions.NavisionFlag	= 0
		AND IIF(NULLIF(@SchemaName,'') IS NULL,'1',@SchemaName) = IIF(NULLIF(@SchemaName,'') IS NULL,'1',SourceObjectDefinitions.SchemaName)
		AND IIF(NULLIF(@TableName,'') IS NULL,'1',@TableName) = IIF(NULLIF(@TableName,'') IS NULL,'1',SourceObjectDefinitions.TableName)
		AND SourceObjectDefinitions.ConnectionType IN ('SqlServer')
	GROUP BY
		 SourceObjectDefinitions.ConnectionType
		,SourceObjectDefinitions.ConnectionName
		,SourceObjectDefinitions.SchemaName
		,SourceObjectDefinitions.TableName
		,SourceObjectDefinitions.ExtractSQLFilter
		,SourceObjectDefinitions.SourceObjectID
		,SourceObjectDefinitions.SourceConnectionID
		,SourceObjectDefinitions.IncrementalFlag
		,SourceObjectDefinitions.KeyColumns
		,SourceObjectDefinitions.IsDateFlag
		,SourceObjectDefinitions.LastValueLoaded
		,SourceObjectDefinitions.IncrementalValueColumnDefinition
GO

