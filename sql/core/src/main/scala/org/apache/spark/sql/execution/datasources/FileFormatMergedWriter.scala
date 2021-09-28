/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormatWriter.OutputSpec
import org.apache.spark.sql.execution.streaming.MetadataLogFileIndex
import org.apache.spark.sql.internal.SQLConf

/** A helper object for writing FileFormat data out to a location. */
case class FileFormatMergedWriter(
    className: String,
    jobId: String,
    outputPath: String,
    dynamicPartitionOverwrite: Boolean = false
) extends Logging {

  private lazy val mergeOutPutPath: String = if (!SQLConf.get.mergeFilesEnable) {
    outputPath
  } else {
    getMergingDir(outputPath, jobId).toString
  }

  private lazy val tmpOutPutPath: String = if (!SQLConf.get.mergeFilesEnable) {
    outputPath
  } else {
    getTmpDir(outputPath, jobId).toString
  }

  lazy val committer: FileCommitProtocol = FileCommitProtocol.instantiate(className, jobId,
    tmpOutPutPath, dynamicPartitionOverwrite)


  private def getMergingDir(path: String, jobId: String): Path = {
    new Path(path, ".spark-merging-" + jobId)
  }

  private def getTmpDir(path: String, jobId: String): Path = {
    new Path(path, ".spark-tmp-" + jobId)
  }


  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[WriteJobStatsTracker],
      options: Map[String, String])
  : Set[String] = {


    val updatedPartitionPaths = FileFormatWriter.write(
      sparkSession,
      plan,
      fileFormat,
      committer,
      outputSpec = outputSpec.copy(outputPath = tmpOutPutPath),
      hadoopConf,
      partitionColumns,
      bucketSpec,
      statsTrackers,
      options)

    mergeSmallFiles(sparkSession,
      plan,
      fileFormat,
      partitionColumns,
      bucketSpec,
      outputSpec,
      options)

    updatedPartitionPaths
  }

  def mergeSmallFiles(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      outputSpec: OutputSpec,
      options: Map[String, String]): Unit = {

    val fileCatalog = new MetadataLogFileIndex(
      sparkSession,
      new Path(tmpOutPutPath),
      options,
      None)
    val dataSchema = outputSpec.outputColumns.filterNot(partitionColumns.contains).toStructType

    val relation = HadoopFsRelation(
      fileCatalog,
      partitionSchema = fileCatalog.partitionSchema,
      dataSchema = dataSchema,
      bucketSpec = None,
      fileFormat,
      options)(sparkSession)


  }
}
