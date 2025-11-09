CREATE VIEW [utility].[SourceInformationSchemaDefinitions]

AS
    
    
    SELECT DISTINCT
             SourceConnectionID                 = SourceObjects.SourceConnectionID
            ,ConnectionName                     = SourceConnections.Name
            ,SourceObjectID                     = SourceObjects.ID
            ,SchemaName                         = SourceInformationSchema.SchemaName
            ,TableName                          = SourceInformationSchema.TableName
            ,AliasTableName                     = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SourceInformationSchema.TableName,')',''),'(',''),'[',''),']',''),',',''),';',''),'{',''),'}',''),' ','_'),'æ','ae'),'ø','oe'),'å','aa')
            ,KeySequenceNumber                  = IIF(SourceInformationSchema.KeySequenceNumber IS NULL AND SourceObjectKeyColumns.KeyColumnName IS NOT NULL,1,SourceInformationSchema.KeySequenceNumber)
            ,SourceColumnName                   = SourceInformationSchema.ColumnName
            ,AliasColumnName                    = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SourceInformationSchema.ColumnName,')',''),'(',''),'[',''),']',''),',',''),';',''),'{',''),'}',''),' ','_'),'æ','ae'),'ø','oe'),'å','aa')
            ,DataTypeName                       = SourceInformationSchema.DataTypeName          
    FROM 
        utility.SourceInformationSchema WITH (NOLOCK)
    INNER JOIN
        meta.SourceObjects WITH (NOLOCK)
            ON  SourceObjects.SchemaName = SourceInformationSchema.SchemaName 
                AND SourceObjects.ObjectName = SourceInformationSchema.TableName
                AND SourceObjects.SourceConnectionID = SourceInformationSchema.SourceConnectionID
    INNER JOIN
        meta.SourceConnections WITH (NOLOCK)
            ON SourceConnections.ID = SourceObjects.SourceConnectionID
    LEFT JOIN
        meta.SourceObjectKeyColumns WITH (NOLOCK)
            ON SourceObjectKeyColumns.SourceObjectID = SourceObjects.ID
            AND SourceObjectKeyColumns.KeyColumnName = SourceInformationSchema.ColumnName
    LEFT JOIN 
        (
            select 
                SourceObjectID,
                count(*) as SourceColumnCount
            from 
                meta.SourceColumns
            group by 
                SourceObjectID
        ) as SourceColumnsDefined ON
            SourceObjects.ID = SourceColumnsDefined.SourceObjectID
    LEFT JOIN
        meta.SourceColumns WITH (NOLOCK)
            ON SourceObjects.ID = SourceColumns.SourceObjectID 
            AND SourceInformationSchema.ColumnName = SourceColumns.ColumnName
            AND SourceColumnsDefined.SourceObjectID is not null
    WHERE
        (
            (SourceColumnsDefined.SourceObjectID is not null and SourceColumns.ColumnName is not null) or -- Included in SourceColumns
            (IIF(SourceInformationSchema.KeySequenceNumber IS NULL AND SourceObjectKeyColumns.KeyColumnName IS NOT NULL,1,SourceInformationSchema.KeySequenceNumber) != '') or -- Part of key sequence
            (SourceColumnsDefined.SourceObjectID is null) -- Include all
        )
        AND SourceObjects.IncludeFlag = 1
        AND SourceConnections.IncludeFlag = 1
        AND SourceInformationSchema.DataTypeName NOT IN ('image')
GO

