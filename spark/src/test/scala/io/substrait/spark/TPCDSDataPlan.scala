/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.sql.{Dataset, DatasetUtil, Row, TPCDSBase}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import com.teradata.tpcds.{Options, Table, TableGenerator}
import io.substrait.plan.{PlanProtoConverter, ProtoPlanConverter}

import java.nio.file.Path

class TPCDSDataPlan extends TPCDSBase with SubstraitPlanTestBase {
  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")

    conf.setConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED, false)
    spark.conf.set("spark.sql.readSideCharPadding", "false")
    spark.conf.set("spark.sql.legacy.charVarcharAsString", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val dir = "build/tmp"
    generateData(dir)
    val tpcdsData = loadData(dir)
    tpcdsData.foreach { case (name, table) => table.createOrReplaceTempView(name) }
  }

  tpcdsQueries.foreach {
    q =>
      test(s"check with data (tpcds-v1.4/$q)") {
        testQueryWithData(q)
      }
  }

  def generateData(dir: String): Unit = {
    val options = new Options()
    options.scale = 0.01
    options.directory = dir
    options.overwrite = true
    val tableGenerator = new TableGenerator(options.toSession)
    Table.getBaseTables.forEach(t => tableGenerator.generateTable(t))
  }

  def loadData(dir: String): Map[String, Dataset[Row]] = {
    tableColumns.map {
      case (tableName, columns) =>
        val schema = StructType.fromDDL(columns)
        val csvPath = Path.of(dir, tableName + ".dat").toAbsolutePath.toString
        val table = spark.read
          .schema(schema)
          .option("delimiter", "|")
          .csv(csvPath)
        (tableName, table)
    }
  }

  def testQueryWithData(queryName: String): Dataset[Row] = {
    val queryString = resourceToString(
      s"tpcds/$queryName.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    val plan = spark.sql(queryString)
    assertRoundTrip(plan)
  }

  def assertRoundTrip(data: Dataset[Row]): Dataset[Row] = {
    val toSubstrait = new ToSubstraitRel
    val sparkPlan = data.queryExecution.optimizedPlan
    val substraitPlan = toSubstrait.convert(sparkPlan)

    // Serialize to proto buffer
    val bytes = new PlanProtoConverter()
      .toProto(substraitPlan)
      .toByteArray

    // Read it back
    val protoPlan = io.substrait.proto.Plan
      .parseFrom(bytes)
    val substraitPlan2 = new ProtoPlanConverter(SparkExtension.COLLECTION).from(protoPlan)

    val sparkPlan2 = new ToLogicalPlan(spark).convert(substraitPlan2)
    val result = DatasetUtil.fromLogicalPlan(spark, sparkPlan2)

    assertResult(data.columns)(result.columns)
    assertResult(data.count)(result.count)
    data.collect().zip(result.collect()).foreach {
      case (before, after) => assertResult(before)(after)
    }
    result
  }

}
