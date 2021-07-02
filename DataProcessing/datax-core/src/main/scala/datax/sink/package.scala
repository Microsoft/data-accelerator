// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
package datax

import java.sql.Timestamp

import datax.config.SettingDictionary
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

package object sink {
  type JsonSinkDelegate = (Row, Seq[Row], Timestamp, Int, String)=>Map[String, Int]
  type SinkDelegate = (DataFrame, Timestamp, String)=>Map[String, Int]
  type Metrics = Map[String, Double]

  trait SinkOperatorFactory{
    def getSinkOperator(dict:SettingDictionary, name: String):SinkOperator

    def getSettingNamespace(): String
  }

  case class SinkOperator(name: String,
                          isEnabled: Boolean,
                          sinkAsJson: Boolean,
                          flagColumnExprGenerator: () => String,
                          generator: (Int)=>SinkDelegate,
                          onInitialization: (SparkSession)=>Unit = null,
                          onBatch: (SparkSession, Timestamp, Set[String])=>Unit = null
                         )

  case class OutputOperator(name: String,
                            onInitialization: (SparkSession) => Unit,
                            onBatch: (SparkSession, Timestamp, Set[String]) => Unit,
                            output: (DataFrame, Timestamp) => Map[String, Int],
                            sinkNames: Seq[String])
}

