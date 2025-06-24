package me.caselastfm.etl.spark_operations.datasets

import me.caselastfm.etl.common_spark.{ConfigParser, IOUtils}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{concat_ws, count, desc, lag, lit, sum}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SQLContext}

class LastFMSessionsOps(implicit spark: SQLContext) {

  import spark.implicits._

  def description: Option[String] = Some(
    """
      |LastFMSessionsOps:
      | This class is composed by a Spark Transformations (let's call operations or Ops, e.g: SparkOps).
      | It should provide the necessary transformations to generate a csv file containing the top 10 songs played in the
      |  top 50 longest sessions (by track count).
      |""".stripMargin
  )

  /** definition is a method responsible to wrap DataFrame transformations into a single place.
   *
   * @return a DataFrame with the results of transformations.
   */
  def definition: DataFrame = {
    val sourceDF: DataFrame = IOUtils.loadCsvFileFromPath(ConfigParser.dataSource, schema, "\t")

    val sessionsDF: DataFrame =
      sourceDF
        .transform(withSessionId())
        .select("user_id", "session_id", "track_name", "artist_name")

    val topLongestSessions: DataFrame =
      sessionsDF
        .transform(withTopSessionsDurationByTrackCount())
        .select("session_id")

    val topSessionsData: DataFrame =
      sessionsDF
        .join(topLongestSessions, Seq("session_id"), "inner")

    topSessionsData
      .transform(withTopSongsFromTopSessions())
  }


  /** A function that returns a session_id per user.
   *
   * @param sessionThreshold [OPTIONAL] the amount of time in minutes that represents a difference between each song that composes
   *                         a session, default 20.
   *                         e.g.: if 20, we say that each session is composed by songs played within 20 minute interval;
   * @param df a DataFrame to be transformed;
   * @return a DataFrame containing a new column named session_id.
   * */
  def withSessionId(sessionThreshold: Long = 20)(df: DataFrame): DataFrame ={

    val window = Window.partitionBy("user_id").orderBy("timestamp")

    df.orderBy("timestamp")
      .withColumn("previous_timestamp", lag($"timestamp".cast("long"), 1, 0).over(window))
      .withColumn("diff_minutes", ($"timestamp".cast("long") - $"previous_timestamp") / 60D)
      .withColumn("fl_new_session", ($"diff_minutes".cast(LongType) > sessionThreshold).cast(IntegerType))
      .withColumn("session_id", concat_ws("_", $"user_id", lit("session"), sum("fl_new_session").over(window)))
      .drop("previous_timestamp", "diff_minutes", "fl_new_session")
  }

  /** A function that returns a DataFrame containing the top N longest sessions.
   *
   * @param thresholdTopSessions [OPTIONAL] integer value that will limit results to the top N sessions, default 50
   * @param df a DataFrame to be transformed
   * */
  def withTopSessionsDurationByTrackCount(thresholdTopSessions: Int = 50)(df: DataFrame): DataFrame = {

    df.select($"session_id", $"track_name")
      .groupBy($"session_id")
      .agg(count("track_name").alias("tracks_count"))
      .orderBy(desc("tracks_count"))
      .limit(thresholdTopSessions)
  }

  /** A function that returns a DataFrame containing the top N songs from top sessions.
   *
   * @param thresholdTopSongs [OPTIONAL] integer value that will limit results to the top N songs, default 10
   * @param df a DataFrame to be transformed
   * */
  def withTopSongsFromTopSessions(thresholdTopSongs: Int = 10)(df: DataFrame): DataFrame = {

    df.select($"track_name", $"artist_name")
      .groupBy($"track_name", $"artist_name")
      .agg(count("track_name").alias("play_count"))
      .orderBy(desc("play_count"))
      .limit(thresholdTopSongs)
  }

  private lazy val schema: StructType = StructType(
    Array(
      StructField("user_id", StringType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("musicbrainz_artist_id", StringType, nullable = true),
      StructField("artist_name", StringType, nullable = false),
      StructField("musicbrainz_track_id", StringType, nullable = true),
      StructField("track_name", StringType, nullable = false)
    )
  )

}
