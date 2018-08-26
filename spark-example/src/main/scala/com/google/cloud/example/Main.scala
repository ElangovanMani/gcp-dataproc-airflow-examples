/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.example

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat
import com.google.gson.JsonObject
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Main {

  def jsonObjectToElement(json: JsonObject): Element = {
    Element(
      json.get("id").getAsLong,
      json.get("col1").getAsString,
      json.get("col2").getAsLong,
      json.get("col3").getAsLong
    )
  }

  case class Element(id: Long, col1: String, col2: Long, col3: Long)

  def main(args: Array[String]){
    val partition = args(0)
    val project = args(1)
    val dataset = args(2)
    val table = args(3)
    val bucket = args(4)
    val prefix = args(5)

    val spark = SparkSession
      .builder
      .appName("BigQueryConnectorExample")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    @transient
    val conf = sc.hadoopConfiguration
    val fullyQualifiedInputTableId = s"$project:$dataset.$table"
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, project)
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)
    conf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, s"$prefix/tmp/")
    BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId)

    val tableData: RDD[(LongWritable, JsonObject)] = sc.newAPIHadoopRDD(
      conf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject])

    val ds: Dataset[Element] = tableData.map(x => jsonObjectToElement(x._2)).toDS()

    ds.limit(100).write.parquet(s"gs://$bucket$prefix/output/$partition.parquet")
    spark.stop()
  }
}
