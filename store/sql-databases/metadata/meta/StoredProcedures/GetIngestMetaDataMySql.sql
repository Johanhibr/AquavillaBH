



CREATE PROCEDURE [meta].[GetIngestMetaDataMySql] 

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
		,SqlKeys							= 'SELECT ' + STRING_AGG(IIF(SourceInformationSchemaDefinitions.KeySequenceNumber IS NULL,NULL, CAST(IIF(SourceConnectionDataTypeConversion.ConvertToDataType IS NULL,SourceInformationSchemaDefinitions.SourceColumnName,'CAST(' + SourceInformationSchemaDefinitions.SourceColumnName + ' AS ' + SourceConnectionDataTypeConversion.ConvertToDataType + ')') + ' AS ' + SourceInformationSchemaDefinitions.AliasColumnName AS NVARCHAR(MAX))),', ')	+
											  ' FROM '  + SourceObjectDefinitions.SchemaName + '.' + SourceObjectDefinitions.TableName +
											  ' WHERE 1 = 1' 
													   + IIF(SourceObjectDefinitions.ExtractSQLFilter = '','',' AND ' + SourceObjectDefinitions.ExtractSQLFilter) 
		,SqlNewWatermark					= CASE 
												WHEN IncrementalFlag = 0 THEN 'SELECT 1 AS NewWatermark'
												ELSE 'SELECT IFNULL(CAST(' 
																+ IIF (SourceObjectDefinitions.IsDateFlag = 1,'DATE_FORMAT(MAX(' + SourceObjectDefinitions.IncrementalValueColumnDefinition + '), ''%Y%m%d%H%i%s'') AS UNSIGNED INTEGER),','MAX(' + SourceObjectDefinitions.IncrementalValueColumnDefinition + ')),') 
																+ IIF (SourceObjectDefinitions.IsDateFlag = 1,'''19000101000000''','''0''') + ') AS NewWatermark FROM ' + SourceObjectDefinitions.SchemaName + '.' + SourceObjectDefinitions.TableName
											  END
		,SqlQuery							= 'SELECT ' + 
														STRING_AGG(CAST(IIF(SourceConnectionDataTypeConversion.ConvertToDataType IS NULL,SourceInformationSchemaDefinitions.SourceColumnName,'CAST(' + SourceInformationSchemaDefinitions.SourceColumnName + ' AS ' + SourceConnectionDataTypeConversion.ConvertToDataType + ')') + ' AS ' + SourceInformationSchemaDefinitions.AliasColumnName AS NVARCHAR(MAX)),', ')	+	
											  ' FROM '  + SourceObjectDefinitions.SchemaName + '.' + SourceObjectDefinitions.TableName +
											  ' WHERE 1 = 1' 
													   + IIF(SourceObjectDefinitions.ExtractSQLFilter = '','',' AND ' + SourceObjectDefinitions.ExtractSQLFilter) 
													   + IIF(SourceObjectDefinitions.IncrementalFlag = 0 OR @JobIsIncremental = 0,'',' AND ' + SourceObjectDefinitions.IncrementalValueColumnDefinition 
													   + ' > ' + 
															CASE
																WHEN SourceObjectDefinitions.IsDateFlag = 1 THEN 'STR_TO_DATE('''+ SourceObjectDefinitions.LastValueLoaded +''', ''%Y%m%d%H%i%s'') '
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
		AND SourceObjectDefinitions.ConnectionType IN ('MySql')
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

