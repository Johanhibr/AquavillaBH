CREATE PROCEDURE [meta].[GetSchemaAsJson]
    @ConnectionName NVARCHAR(128),
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON
    SELECT [type] = 'struct',
        (
        SELECT
            [name]      =   [ColumnName],
            [type]      =   IIF(COALESCE([ExportDataTypeName], 'string') = 'decimal', 
                                N'decimal(' + CAST([NumericPrecisionNumber] AS NCHAR(2)) + N',' + CAST([NumericScaleNumber] AS NCHAR(1)) + ')', 
                                COALESCE([ExportDataTypeName], 'string')
                            ),
            [nullable]  =   COALESCE([NullableName], 'false') ,
            [meta]      =   '{ }'
        FROM 
			[utility].[SourceInformationSchema] 
        LEFT JOIN 
			[utility].[JsonSchemaDataMapping] 
				ON  [SourceInformationSchema].[SourceSystemTypeName] = [JsonSchemaDataMapping].[SourceSystemTypeName] 
				AND [SourceInformationSchema].[DataTypeName] = [JsonSchemaDataMapping].[SourceDataTypeName]
        WHERE 
				[SourceInformationSchema].[SourceConnectionName] = @ConnectionName 
			AND [SourceInformationSchema].[SchemaName] = @SchemaName 
			AND [SourceInformationSchema].[TableName] = @TableName
        ORDER BY 
			OrdinalPositionNumber
        FOR JSON PATH
    ) AS [fields]
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
END
GO

