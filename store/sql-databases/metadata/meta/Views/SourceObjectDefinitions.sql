







CREATE VIEW [meta].[SourceObjectDefinitions] AS

		WITH SourceObjectIncrementalSetup AS

			(

			SELECT 
				 SourceObjectID						= SourceObjects.ID 
				,IncrementalValueColumnDefinition	= IncrementalValueColumnDefinition
				,IsDateFlag							= IsDateFlag
				,LastValueLoaded					= CASE 
															WHEN SourceObjectIncrementalSetup.IsDateFlag = 1 THEN FORMAT(DATEADD(DD,ISNULL(SourceObjectIncrementalSetup.RollingWindowDays,0),CONVERT(datetime,STUFF(STUFF(STUFF(ISNULL(SourceObjectIncrementalSetup.LastValueLoaded,'19000101000000'),13,0,':'),11,0,':'),9,0,' '))),'yyyyMMddHHmmss')
															ELSE CAST(CAST(ISNULL(SourceObjectIncrementalSetup.LastValueLoaded,'0') AS BIGINT) + ISNULL(SourceObjectIncrementalSetup.RollingWindowDays,0) AS NVARCHAR(20))
													  END		
			FROM 
				meta.SourceObjects WITH (NOLOCK)
			INNER JOIN
				meta.SourceObjectIncrementalSetup  WITH (NOLOCK)
					ON SourceObjectIncrementalSetup.SourceObjectID = SourceObjects.ID

					)


		SELECT 
			 ConnectionType						= SourceConnections.ConnectionType
            ,ConnectionName						= SourceConnections.Name
			,NavisionFlag						= SourceConnections.NavisionFlag
			,SourceConnectionID					= SourceObjects.SourceConnectionID
			,SchemaName							= SourceObjects.SchemaName
			,TableName							= SourceObjects.ObjectName
			,SourceObjectID						= SourceObjects.ID 
			,IncrementalFlag					= SourceObjects.IncrementalFlag
			,ExtractSQLFilter					= SourceObjects.ExtractSQLFilter
			,IncrementalValueColumnDefinition	= SourceObjectIncrementalSetup.IncrementalValueColumnDefinition
			,IsDateFlag							= SourceObjectIncrementalSetup.IsDateFlag
			,LastValueLoaded					= SourceObjectIncrementalSetup.LastValueLoaded
			,KeyColumns							= '[' + STRING_AGG('"' + ISNULL(IIF(ISNULL(SourceInformationSchemaDefinitions.KeySequenceNumber,'') = '',NULL,SourceInformationSchemaDefinitions.AliasColumnName),REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SourceObjectKeyColumns.KeyColumnName,')',''),'(',''),'[',''),']',''),',',''),';',''),'{',''),'}',''),' ','_'),'æ','ae'),'ø','oe'),'å','aa')) + '"',',' ) + IIF(SourceConnections.NavisionFlag = 1,', "CompanyName"','') + ']'
		FROM 
			meta.SourceObjects WITH (NOLOCK)
		INNER JOIN
			meta.SourceConnections WITH (NOLOCK)
				ON SourceConnections.ID = SourceObjects.SourceConnectionID
		LEFT JOIN
			utility.SourceInformationSchemaDefinitions
				ON  SourceObjects.ID = SourceInformationSchemaDefinitions.SourceObjectID 
				AND ISNULL(SourceInformationSchemaDefinitions.KeySequenceNumber,'') <> ''
		LEFT JOIN
			SourceObjectIncrementalSetup
				ON SourceObjectIncrementalSetup.SourceObjectID = SourceObjects.ID
		LEFT JOIN 
			meta.SourceObjectKeyColumns WITH (NOLOCK)
				ON  SourceObjectKeyColumns.SourceObjectID = SourceObjects.ID
				AND SourceObjects.KeyColumnFlag = 1
				AND SourceObjectKeyColumns.KeyColumnName = SourceInformationSchemaDefinitions.SourceColumnName
		WHERE
			SourceObjects.IncludeFlag = 1 
			AND SourceConnections.IncludeFlag = 1
		GROUP BY 
			 SourceConnections.ConnectionType
			,SourceConnections.Name
			,SourceConnections.NavisionFlag
			,SourceObjects.SourceConnectionID
			,SourceObjects.SchemaName
			,SourceObjects.ObjectName
			,SourceObjects.ID 
			,SourceObjects.IncrementalFlag
			,SourceObjects.ExtractSQLFilter
			,SourceObjectIncrementalSetup.IncrementalValueColumnDefinition
			,SourceObjectIncrementalSetup.IsDateFlag
			,SourceObjectIncrementalSetup.LastValueLoaded
GO

