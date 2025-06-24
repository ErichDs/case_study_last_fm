package me.caselastfm.spark_operations.datasets

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object TestHelpers {

  def setMapTypeNullable(mapType: MapType, valueContainsNull: Boolean = true): MapType = mapType match {
    case MapType(k, v, _) =>
      MapType(k, setDataTypeNullable(v, valueContainsNull), valueContainsNull = valueContainsNull)
  }

  def setArrayTypeNullable(arrayType: ArrayType, nullable: Boolean): ArrayType = arrayType match {
    case ArrayType(t, _) => ArrayType(setDataTypeNullable(t, nullable), nullable)
  }

  def setDataTypeNullable(dataType: DataType, nullable: Boolean): DataType = dataType match {
    case mt: MapType    => setMapTypeNullable(mt, nullable)
    case st: StructType => setStructTypeNullable(st, nullable)
    case at: ArrayType  => setArrayTypeNullable(at, nullable)
    case _              => dataType
  }

  def setFieldNullable(fieldType: StructField, nullable: Boolean): StructField = fieldType match {
    case StructField(c, t, _, m) => StructField(c, setDataTypeNullable(t, nullable), nullable, m)
  }

  def setStructTypeNullable(structType: StructType, nullable: Boolean): StructType = {
    StructType(structType.map(f => setFieldNullable(f, nullable)))
  }

  def setAllNullables(df: DataFrame, nullable: Boolean): DataFrame ={
    val schema = df.schema
    val newSchema = setStructTypeNullable(schema, nullable)
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  def diffDataFrames(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.union(df2).except(df1.intersect(df2))
  }

}
