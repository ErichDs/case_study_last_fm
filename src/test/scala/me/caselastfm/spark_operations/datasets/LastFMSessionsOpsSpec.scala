package me.caselastfm.spark_operations.datasets

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import me.caselastfm.etl.spark_operations.datasets.LastFMSessionsOps
import TestHelpers.setAllNullables
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.TimestampType
import org.scalatest.flatspec.AnyFlatSpec

class LastFMSessionsOpsSpec extends AnyFlatSpec with DataFrameSuiteBase with SparkSessionTest {

  import spark.implicits._

  val timestampFormat: String = "yyyy-MM-dd'T'HH:mm:ss'Z'"

  val sparkOp: LastFMSessionsOps = new LastFMSessionsOps()

  "withSessionId" should "return a dataframe containing a new column called session_id" in {

    val inputDF: DataFrame = Seq(
      ("user_01", "2024-01-01T00:00:01Z", "iron maiden", "fear of the dark"),
      ("user_01", "2024-01-01T00:06:00Z", "linkin park", "faint"),
      ("user_01", "2024-01-01T00:08:21Z", "metallica", "whiskey in the jar"),
      ("user_01", "2024-01-02T00:12:00Z", "ozzy osbourne", "crazy train"),

      ("user_02", "2024-01-01T02:35:00Z", "dystopia", "megadeth"),
      ("user_02", "2024-01-01T02:38:17Z", "unleashed", "epica"),
      ("user_02", "2024-01-02T14:22:01Z", "dragonforce", "trough the fire and flames"),

      ("user_03", "2023-09-05T00:00:01Z", "lacuna coil", "spellbound"),
      ("user_03", "2024-02-26T19:41:01Z", "korn", "freak on a leash"),
      ("user_03", "2024-02-26T19:44:01Z", "slipknot", "duality"),
      ("user_03", "2024-04-15T00:00:01Z", "iron maiden", "flash of the blade"),
    ).toDF("user_id", "timestamp", "artist_name", "track_name")
      .withColumn("timestamp", to_timestamp($"timestamp", timestampFormat).cast(TimestampType))

    val expectedDF: DataFrame = Seq(
      ("user_01", "2024-01-01T00:00:01Z", "iron maiden", "fear of the dark", "user_01_session_1"),
      ("user_01", "2024-01-01T00:06:00Z", "linkin park", "faint", "user_01_session_1"),
      ("user_01", "2024-01-01T00:08:21Z", "metallica", "whiskey in the jar", "user_01_session_1"),
      ("user_01", "2024-01-02T00:12:00Z", "ozzy osbourne", "crazy train", "user_01_session_2"),

      ("user_02", "2024-01-01T02:35:00Z", "dystopia", "megadeth", "user_02_session_1"),
      ("user_02", "2024-01-01T02:38:17Z", "unleashed", "epica", "user_02_session_1"),
      ("user_02", "2024-01-02T14:22:01Z", "dragonforce", "trough the fire and flames", "user_02_session_2"),

      ("user_03", "2023-09-05T00:00:01Z", "lacuna coil", "spellbound", "user_03_session_1"),
      ("user_03", "2024-02-26T19:41:01Z", "korn", "freak on a leash", "user_03_session_2"),
      ("user_03", "2024-02-26T19:44:01Z", "slipknot", "duality", "user_03_session_2"),
      ("user_03", "2024-04-15T00:00:01Z", "iron maiden", "flash of the blade", "user_03_session_3"),
    ).toDF("user_id", "timestamp", "artist_name", "track_name", "session_id")
      .withColumn("timestamp", to_timestamp($"timestamp", timestampFormat).cast(TimestampType))

    val resultDF: DataFrame = sparkOp.withSessionId()(inputDF)

    assertDataFrameEquals(setAllNullables(expectedDF, nullable = true), setAllNullables(resultDF, nullable = true))
  }

  "withTopSessionsDurationByTrackCount" should "return a dataframe containing top N sessions by track count" in {

    val inputDF: DataFrame = Seq(
      ("user_01_session_id_1", "fear of the dark"),
      ("user_01_session_id_1", "unforgiven"),
      ("user_01_session_id_1", "blank infinity"),
      ("user_01_session_id_1", "one"),
      ("user_02_session_id_1", "fear of the dark"),
      ("user_02_session_id_1", "dystopia"),
      ("user_02_session_id_2", "tornado of souls"),
      ("user_03_session_id_1", "mirrors"),
      ("user_03_session_id_1", "crazy in love"),
      ("user_03_session_id_1", "what goes around/comes around"),
      ("user_03_session_id_1", "my love"),
      ("user_03_session_id_1", "poker face"),
      ("user_04_session_id_1", "one"),
      ("user_04_session_id_2", "nothing else matters"),
      ("user_04_session_id_3", "unleashed")
    ).toDF("session_id", "track_name")

    val expectedDF: DataFrame = Seq(
      ("user_03_session_id_1", 5L),
      ("user_01_session_id_1", 4L),
      ("user_02_session_id_1", 2L)
    ).toDF("session_id", "tracks_count")

    val resultDF: DataFrame = sparkOp.withTopSessionsDurationByTrackCount(3)(inputDF)

    assertDataFrameEquals(expectedDF, resultDF)
  }

  "withTopSongsFromTopSessions" should "return top X songs from top Y sessions by track count" in {

    val inputDF: DataFrame = Seq(
      ("user_01_session_id_1", "fear of the dark", "iron maiden"),
      ("user_01_session_id_13", "unforgiven", "metallica"),
      ("user_01_session_id_19", "blank infinity", "epica"),
      ("user_01_session_id_1", "one", "metallica"),
      ("user_02_session_id_8", "fear of the dark", "iron maiden"),
      ("user_02_session_id_11", "whiskey in the jar", "metallica"),
      ("user_02_session_id_1", "one", "metallica"),
      ("user_02_session_id_22", "tornado of souls", "megadeth"),
      ("user_03_session_id_1", "mirrors", "justin timberlake"),
      ("user_03_session_id_14", "unforgiven", "metallica"),
      ("user_03_session_id_31", "what goes around/comes around", "justin timberlake"),
      ("user_03_session_id_1", "fear of the dark", "iron maiden"),
      ("user_03_session_id_4", "my love", "justin timberlake"),
      ("user_03_session_id_15", "tornado of souls", "megadeth"),
      ("user_04_session_id_2", "one", "metallica"),
      ("user_04_session_id_1", "fear of the dark", "iron maiden"),
      ("user_04_session_id_2", "nothing else matters", "metallica"),
      ("user_04_session_id_3", "unleashed", "epica"),
      ("user_05_session_id_21", "fear of the dark", "iron maiden"),
      ("user_05_session_id_3", "one", "metallica"),
      ("user_05_session_id_6", "tornado of souls", "megadeth"),
    ).toDF("session_id", "track_name", "artist_name")

    val expectedDF: DataFrame = Seq(
      ("fear of the dark", "iron maiden", 5L),
      ("one", "metallica", 4L),
      ("tornado of souls", "megadeth", 3L),
      ("unforgiven", "metallica", 2L)
    ).toDF("track_name", "artist_name", "play_count")

    val resultDF: DataFrame = sparkOp.withTopSongsFromTopSessions(4)(inputDF)

    assertDataFrameEquals(expectedDF, resultDF)
  }

}
