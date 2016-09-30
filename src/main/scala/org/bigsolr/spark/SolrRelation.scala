package org.bigsolr.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bigsolr.hadoop.{SolrInputFormat, SolrRecord}

import scala.language.existentials


case class SolrRelation(
                         query: String,
                         serverUrl: String,
                         serverMode: String,
                         collection: String,
                         fields: String
                       )(@transient val sqlContext: SQLContext) extends BaseRelation with PrunedFilteredScan {

  val schema = {
    StructType(fields.split(",").map(fieldName => StructField(fieldName, StringType, true)))
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]) = {

    // Build the job configuration
    var conf = new Configuration()
    conf.set("solr.query", query)
    conf.set("solr.server.url", serverUrl)
    conf.set("solr.server.mode", serverMode)
    conf.set("solr.server.collection", collection)
    conf.set("solr.server.fields", fields)

    val rdds = sqlContext.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[SolrInputFormat],
      classOf[NullWritable],
      classOf[SolrRecord]
    )

    rdds.map {
      case (key, value) => {
        val row = scala.collection.mutable.ListBuffer.empty[String]
        requiredColumns.foreach { field =>
          row += value.getFieldValues(field).toString()
        }

        Row.fromSeq(row)
      }
    }

  }

}