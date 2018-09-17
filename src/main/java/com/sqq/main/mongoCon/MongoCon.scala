package com.sqq.main.mongoCon

import com.mongodb.client.MongoDatabase
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig

case class MongoCon(collectionName: String, databaseName: String, mongoDBList: String) {

  def writeConfig: WriteConfig = {
    val uri = "mongodb://" + mongoDBList
    val options = scala.collection.mutable.Map(
      "database" -> databaseName,
      //大多数情况下从primary的replica set读，当其不可用时，从其secondary members读
      "readPreference.name" -> "primaryPreferred",
      "collection" -> collectionName,
      "spark.mongodb.input.uri" -> uri,
      "spark.mongodb.output.uri" -> uri)

    WriteConfig(options)
  }

  def databaseWrite[T](code: MongoDatabase => T): T = {
    val wConfig = writeConfig
    val mongoConnector = MongoConnector(wConfig.asOptions)
    mongoConnector.withDatabaseDo(wConfig, code)
  }
}
