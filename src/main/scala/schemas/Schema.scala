package schemas

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Schema {
//object Schema extends App{

  def getSchema()= {


    val columnNames =
      """ Call Number,Unit ID,Incident Number,Call Type,Call Date,Watch Date,Received DtTm,Entry DtTm,Dispatch DtTm,Response DtTm,On Scene DtTm,Transport DtTm,Hospital DtTm,Call Final Disposition,Available DtTm,Address,City,Zipcode of Incident,Battalion,Station Area,Box,Original Priority,Priority,Final Priority,ALS Unit,Call Type Group,Number of Alarms,Unit Type,Unit sequence in call dispatch,Fire Prevention District,Supervisor District,Neighborhooods - Analysis Boundaries,Location,RowID """
    val schema = StructType(
      columnNames.trim.split(",")
        .map { name =>
          val col = name.replace(" ", "").toLowerCase.replace(" ", "")
          StructField(col, StringType)
        }

    )
schema
  }
  getSchema
}
