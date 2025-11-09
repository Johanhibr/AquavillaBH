



CREATE PROCEDURE [meta].[GetIngestMetaDataOdata] 

	@ConnectionName NVARCHAR(50),
	@TableName NVARCHAR(50)

AS


	SELECT 
		 ConnectionType						= SourceObjectDefinitions.ConnectionType
		,ConnectionName						= SourceObjectDefinitions.ConnectionName
		,SourceConnectionID					= SourceObjectDefinitions.SourceConnectionID
		,TableName							= SourceObjectDefinitions.TableName
		,TableNameAlias						= SourceObjectDefinitions.TableName
		,SourceObjectID						= SourceObjectDefinitions.SourceObjectID 
		,IncrementalFlag					= SourceObjectDefinitions.IncrementalFlag
		,LoadType							= 'Full'
		,JobIsIncremental					= CAST(0 AS BIT)
		,KeyColumns							= SourceObjectDefinitions.KeyColumns
		,NewWatermark						= ''											  	

	FROM 		
		meta.SourceObjectDefinitions
	LEFT JOIN
		utility.SourceInformationSchemaDefinitions
			ON SourceInformationSchemaDefinitions.SourceObjectID = SourceObjectDefinitions.SourceObjectID
	WHERE
			SourceObjectDefinitions.ConnectionName	= @ConnectionName
		AND IIF(NULLIF(@TableName,'') IS NULL,'1',@TableName) = IIF(NULLIF(@TableName,'') IS NULL,'1',SourceObjectDefinitions.TableName)
		AND SourceInformationSchemaDefinitions.SourceObjectID IS NULL
		AND SourceObjectDefinitions.ConnectionType = 'Odata'
GO

