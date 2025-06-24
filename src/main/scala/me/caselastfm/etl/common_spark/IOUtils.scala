package me.caselastfm.etl.common_spark

import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode}
import org.apache.spark.sql.types.StructType

object IOUtils {

  /** A function to load generic files from a specifc path.
   *
   * @param filepath STRING the path with the filename to be loaded
   * @param schema STRUCTTYPE file schema to be parsed on loading
   * @param delimiter [OPTIONAL] STRING the delimiter/separator of the file, default ";"
   * @param header [OPTIONAL] BOOLEAN flag true/false that indicates if a header should be loaded, default "false"
   * @return DataFrame containing loaded data
   * */
  def loadCsvFileFromPath(filepath: String, schema: StructType, delimiter: String = ";", header: Boolean = false)
                             (implicit sqlContext: SQLContext): DataFrame ={
    sqlContext.read
      .format("csv")
      .option("delimiter", delimiter)
      .option("header", header)
      .schema(schema)
      .load(filepath)
  }

  /** A function that writes a DF to a csv.
   *
   * @param df DataFrame to be written as csv file
   * @param outputPath STRING filepath where data can be found
   * @param delimiter [OPTIONAL] STRING delimiter/separator to the file data, default ';'
   * @param header [OPTIONAL] BOOLEAN flag to include header in the file, default 'true'
   * */
  def writeDF(df: Dataset[_ <: Serializable], outputFileName: String, outputPath: String, delimiter: String = ";",
              header: Boolean = true): Unit = {
    df.coalesce(1)
      .write
      .format("csv")
      .option("delimiter", delimiter)
      .option("header", header)
      .mode(SaveMode.Overwrite)
      .save(outputPath + outputFileName)
  }
}
