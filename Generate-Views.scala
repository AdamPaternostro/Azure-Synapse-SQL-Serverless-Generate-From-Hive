/*
Run this code from a Spark notebook (Scala)
*/

val dfTable = spark.sql("SHOW TABLES").filter("tableName == 'REPLACE-WITH-TABLE-NAME'") // you can run for all tables, but better to test with a few first
  
for (row <- dfTable.collect()) 
{   
    var sqlOD = new StringBuilder(); 
    var sqlODPartitionSelect = new StringBuilder(); 
    var sqlODPartitionLocation = new StringBuilder(); 
    var sqlODRemovePartitionColFromTable = new StringBuilder(); 
    sqlOD.clear()
    sqlODPartitionSelect.clear()
    sqlODPartitionLocation.clear()
    sqlODRemovePartitionColFromTable.clear()

    var database = row.mkString(",").split(",")(0)
    var tableName = row.mkString(",").split(",")(1)

    val location = spark.sql("DESCRIBE EXTENDED " + tableName).filter("col_name == 'Location'").select($"data_type")
    
    sqlOD.append("-----------------------------------------------------------------------------------------------\n")
    sqlOD.append("-- " + tableName + "\n")
    sqlOD.append("-----------------------------------------------------------------------------------------------\n")
    sqlOD.append("IF EXISTS (SELECT * FROM sys.objects WHERE name ='" + tableName + "')\n")
    sqlOD.append("  BEGIN\n")
    sqlOD.append("  DROP VIEW " + tableName + ";\n")
    sqlOD.append("  END\n")
    sqlOD.append("GO\n\n")
    sqlOD.append("CREATE VIEW " + tableName + " AS\n")
    sqlOD.append("SELECT\n")
    sqlOD.append("REPLACE_ME_sqlODPartitionSelect")
    sqlOD.append("  *\n")
    sqlOD.append("FROM OPENROWSET(\n")
    sqlOD.append("   BULK '")
    for (loc <- location.collect())
    {
        var abfsPath = loc.mkString(",").split(",")(0)
        sqlOD.append(abfsPath)
        sqlOD.append("REPLACE_ME_sqlODPartitionLocation")
    }
    sqlOD.append("/*.parquet',\n")
    sqlOD.append("   FORMAT='PARQUET'\n")
    sqlOD.append(")\n")
    sqlOD.append("WITH (\n")

    val cols = spark.sql("DESCRIBE " + tableName)
    var i = 1;
    var partitionIndex = 1;
    var numberOfColumns = cols.collect().length
    var processingPartitions = false

    for (col <- cols.collect())
    {
        var colString = col.mkString("|")
        //println(colString)
        var colSplit = colString.split('|')
        //colSplit.foreach(println) 
        var col_name = colSplit(0)
        var data_type = ""
        try
        {
            data_type = colSplit(1)
        }
        catch {
            case e: Exception => var void = ""
        }

        // Map data types (MORE WORK needs to be done here, this is just a few mappings)
        if (data_type == "string")
        {
            data_type = "VARCHAR(255)"    // NOTE: This is just a fixed number that was picked since it covered most of the string sizes.  
        }
        if (data_type == "double")
        {
            data_type = "FLOAT"
        }
         if (data_type == "timestamp")
        {
            data_type = "DATETIME2"
        }      

        if (col_name == "# Partition Information" || col_name== "# col_name")
        {
            processingPartitions = true;
        }

        if (processingPartitions == true && col_name != "# Partition Information" && col_name != "# col_name")
        {
            sqlODPartitionSelect.append("   CAST(r.filepath(" + partitionIndex.toString() + ") AS " + data_type + ") AS " + col_name + ",\n")
            sqlODPartitionLocation.append("/" + col_name + "=*")
            partitionIndex = partitionIndex + 1 
            sqlODRemovePartitionColFromTable.append("  " + col_name + " " + data_type).append(",\n")
       }
        
        if (processingPartitions == false)
        {
            sqlOD.append("  " + col_name + " " + data_type).append(",\n")
        }
        i = i + 1
    }
          
    sqlOD.append(") AS [r];\n")
    sqlOD.append("GO\n\n")
    sqlOD.append("-- SELECT TOP 10 * FROM " + tableName + ";\n")
    sqlOD.append("-----------------------------------------------------------------------------------------------\n")
    sqlOD.append("\n")
    sqlOD.append("\n")

    println(sqlOD.toString()
    .replace("REPLACE_ME_sqlODPartitionSelect",sqlODPartitionSelect.toString())
    .replace("REPLACE_ME_sqlODPartitionLocation",sqlODPartitionLocation.toString())
    .replace(sqlODRemovePartitionColFromTable.toString(),"") // remove partition columns from WITH statement
    .replace(",\n) AS [r];","\n) AS [r];") // remove the trailing comma from the WITH statement
    )
}