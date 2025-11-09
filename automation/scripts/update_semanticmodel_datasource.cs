using System.Text.RegularExpressions;

string sqlEndpointConnectionString = "@sqlEndpointConnectionString";
string sqlEndpointId = "@sqlEndpointId";
string lakehouseName = "@lakehouseName";

const string lakehouseURL = @"""{0}"" meta [IsParameterQuery=true, Type=""Text"", IsParameterQueryRequired=true]";
string newLakehouseURL = string.Format(lakehouseURL, sqlEndpointConnectionString);

Model.Expressions["LakehouseURL"].Expression = newLakehouseURL;

// Define regex pattern to match Sql.Database("oldSqlEndpoint", "oldLakehouse")
string pattern = @"Sql\.Database\(""([^""]+)"",\s*""([^""]+)""\)";

foreach ( var table in Model.Tables )
{
    foreach ( var partion in table.Partitions )
    {
        if ( partion.SourceType == PartitionSourceType.M )
        {
            // Replace using regex
            string updatedMQuery = Regex.Replace(partion.Expression, pattern, "Sql.Database(\""+sqlEndpointConnectionString+"\", \""+lakehouseName+"\")");
     
            // Replace the database in partition expressions
            partion.Expression = updatedMQuery;
        }
    }
}