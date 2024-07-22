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
package io.substrait.spark.logical

import io.substrait.spark.{DefaultRelVisitor, SparkExtension, ToSubstraitType}
import io.substrait.spark.expression._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructType}
import io.substrait.`type`.{StringTypeVisitor, Type}
import io.substrait.{expression => exp}
import io.substrait.expression.{Expression => SExpression}
import io.substrait.plan.Plan
import io.substrait.proto.WriteRel.WriteOp
import io.substrait.relation
import io.substrait.relation.{LocalFiles, VirtualTableScan, Write}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer

/**
 * RelVisitor to convert Substrait Rel plan to [[LogicalPlan]]. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
class ToLogicalPlan(spark: SparkSession) extends DefaultRelVisitor[LogicalPlan] {

  private val expressionConverter =
    new ToSparkExpression(ToScalarFunction(SparkExtension.SparkScalarFunctions), Some(this))

  private def fromMeasure(measure: relation.Aggregate.Measure): AggregateExpression = {
    // this functions is called in createParentwithChild
    val function = measure.getFunction
    var arguments = function.arguments().asScala.zipWithIndex.map {
      case (arg, i) =>
        arg.accept(function.declaration(), i, expressionConverter)
    }
    if (function.declaration.name == "count" && function.arguments.size == 0) {
      // HACK - count() needs to be rewritten as count(1)
      arguments = ArrayBuffer(Literal(1))
    }

    val aggregateFunction = SparkExtension.toAggregateFunction
      .getSparkExpressionFromSubstraitFunc(function.declaration.key, function.outputType)
      .map(sig => sig.makeCall(arguments))
      .map(_.asInstanceOf[AggregateFunction])
      .getOrElse({
        val msg = String.format(
          "Unable to convert Aggregate function %s(%s).",
          function.declaration.name,
          function.arguments.asScala
            .map {
              case ea: exp.EnumArg => ea.value.toString
              case e: SExpression => e.getType.accept(new StringTypeVisitor)
              case t: Type => t.accept(new StringTypeVisitor)
              case a => throw new IllegalStateException("Unexpected value: " + a)
            }
            .mkString(", ")
        )
        throw new IllegalArgumentException(msg)
      })
    AggregateExpression(
      aggregateFunction,
      ToAggregateFunction.toSpark(function.aggregationPhase()),
      ToAggregateFunction.toSpark(function.invocation()),
      None
    )
  }

  private def toNamedExpression(e: Expression): NamedExpression = e match {
    case ne: NamedExpression => ne
    case other => Alias(other, toPrettySQL(other))()
  }

  override def visit(aggregate: relation.Aggregate): LogicalPlan = {
    require(aggregate.getGroupings.size() == 1)
    val child = aggregate.getInput.accept(this)
    withChild(child) {
      val groupBy = aggregate.getGroupings
        .get(0)
        .getExpressions
        .asScala
        .map(expr => expr.accept(expressionConverter))

      val outputs = groupBy.map(toNamedExpression)
      val aggregateExpressions =
        aggregate.getMeasures.asScala.map(fromMeasure).map(toNamedExpression)
      Aggregate(groupBy, outputs ++= aggregateExpressions, child)
    }
  }

  override def visit(join: relation.Join): LogicalPlan = {
    val left = join.getLeft.accept(this)
    val right = join.getRight.accept(this)
    withChild(left, right) {
      val condition = Option(join.getCondition.orElse(null))
        .map(_.accept(expressionConverter))

      val joinType = join.getJoinType match {
        case relation.Join.JoinType.INNER => Inner
        case relation.Join.JoinType.LEFT => LeftOuter
        case relation.Join.JoinType.RIGHT => RightOuter
        case relation.Join.JoinType.OUTER => FullOuter
        case relation.Join.JoinType.SEMI => LeftSemi
        case relation.Join.JoinType.ANTI => LeftAnti
        case relation.Join.JoinType.UNKNOWN =>
          throw new UnsupportedOperationException("Unknown join type is not supported")
      }
      Join(left, right, joinType, condition, hint = JoinHint.NONE)
    }
  }

  override def visit(join: relation.Cross): LogicalPlan = {
    val left = join.getLeft.accept(this)
    val right = join.getRight.accept(this)
    withChild(left, right) {
      // TODO: Support different join types here when join types are added to cross rel for BNLJ
      // Currently, this will change both cross and inner join types to inner join
      Join(left, right, Inner, Option(null), hint = JoinHint.NONE)
    }
  }

  private def toSortOrder(sortField: SExpression.SortField): SortOrder = {
    val expression = sortField.expr().accept(expressionConverter)
    val (direction, nullOrdering) = sortField.direction() match {
      case SExpression.SortDirection.ASC_NULLS_FIRST => (Ascending, NullsFirst)
      case SExpression.SortDirection.DESC_NULLS_FIRST => (Descending, NullsFirst)
      case SExpression.SortDirection.ASC_NULLS_LAST => (Ascending, NullsLast)
      case SExpression.SortDirection.DESC_NULLS_LAST => (Descending, NullsLast)
      case other =>
        throw new RuntimeException(s"Unexpected Expression.SortDirection enum: $other !")
    }
    SortOrder(expression, direction, nullOrdering, Seq.empty)
  }
  override def visit(fetch: relation.Fetch): LogicalPlan = {
    val child = fetch.getInput.accept(this)
    val limit = Literal(fetch.getCount.getAsLong.intValue(), IntegerType)
    fetch.getOffset match {
      case 1L => GlobalLimit(limitExpr = limit, child = child)
      case -1L => LocalLimit(limitExpr = limit, child = child)
      case _ => visitFallback(fetch)
    }
  }
  override def visit(sort: relation.Sort): LogicalPlan = {
    val child = sort.getInput.accept(this)
    withChild(child) {
      val sortOrders = sort.getSortFields.asScala.map(toSortOrder)
      Sort(sortOrders, global = true, child)
    }
  }

  override def visit(project: relation.Project): LogicalPlan = {
    val child = project.getInput.accept(this)
    val (output, createProject) = child match {
      case a: Aggregate => (a.aggregateExpressions, false)
      case other => (other.output, true)
    }

    withOutput(output) {
      val projectList =
        project.getExpressions.asScala
          .map(expr => expr.accept(expressionConverter))
          .map(toNamedExpression)
      if (createProject) {
        Project(projectList, child)
      } else {
        val aggregate: Aggregate = child.asInstanceOf[Aggregate]
        aggregate.copy(aggregateExpressions = projectList)
      }
    }
  }

  override def visit(filter: relation.Filter): LogicalPlan = {
    val child = filter.getInput.accept(this)
    withChild(child) {
      val condition = filter.getCondition.accept(expressionConverter)
      Filter(condition, child)
    }
  }

  override def visit(write: Write): LogicalPlan = {
    val child = write.getInput.accept(this)
    val schema = ToSubstraitType.toStructType(write.getTableSchema)
    val output = schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    val file = write.getFile.get()
    val mode = write.getOperation match {
      case WriteOp.WRITE_OP_INSERT => SaveMode.Append
      case WriteOp.WRITE_OP_UPDATE => SaveMode.Overwrite
    }

    withChild(child) {
      CommandResult(
        output = output,
        commandLogicalPlan = InsertIntoHadoopFsRelationCommand(
          outputPath = new Path(file.getPath.get()),
          staticPartitions = Map(),
          ifPartitionNotExists = false,
          partitionColumns = Seq.empty,
          bucketSpec = None,
          fileFormat = new ParquetFileFormat(),
          options = Map(),
          query = child,
          mode = mode,
          catalogTable = None,
          fileIndex = None,
          outputColumnNames = write.getTableSchema.names.asScala
        ),
        commandPhysicalPlan = null,
        rows = Seq.empty
      )
    }
  }

  override def visit(virtualTableScan: VirtualTableScan): LogicalPlan = {
    LocalRelation.fromExternalRows(
      ToSubstraitType.toAttribute(virtualTableScan.getInitialSchema),
      virtualTableScan.getRows.asScala map(r => {
        val values = r.fields().asScala.map(f => f.accept(expressionConverter)).map(f => f.eval())
        Row.fromSeq(values)
      })
    )
  }

  override def visit(emptyScan: relation.EmptyScan): LogicalPlan = {
    LocalRelation(ToSubstraitType.toAttribute(emptyScan.getInitialSchema))
  }
  override def visit(namedScan: relation.NamedScan): LogicalPlan = {
    resolve(UnresolvedRelation(namedScan.getNames.asScala)) match {
      case m: MultiInstanceRelation => m.newInstance()
      case other => other
    }
  }

  override def visit(localFiles: LocalFiles): LogicalPlan = {
    val schema = ToSubstraitType.toStructType(localFiles.getInitialSchema)
    val output = schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    new LogicalRelation(
      relation = HadoopFsRelation(
        location = new InMemoryFileIndex(
          spark,
          localFiles.getItems.asScala.map(i => new Path(i.getPath.get())),
          Map(),
          Some(schema)),
        partitionSchema = new StructType(),
        dataSchema = schema,
        bucketSpec = None,
        fileFormat = new CSVFileFormat(),
        options = Map()
      )(spark),
      output = output,
      catalogTable = None,
      isStreaming = false
    )
  }

  private def withChild(child: LogicalPlan*)(body: => LogicalPlan): LogicalPlan = {
    val output = child.flatMap(_.output)
    withOutput(output)(body)
  }

  private def withOutput(output: Seq[NamedExpression])(body: => LogicalPlan): LogicalPlan = {
    expressionConverter.pushOutput(output)
    try {
      body
    } finally {
      expressionConverter.popOutput()
    }
  }
  private def resolve(plan: LogicalPlan): LogicalPlan = {
    val qe = new QueryExecution(spark, plan)
    qe.analyzed match {
      case SubqueryAlias(_, child) => child
      case other => other
    }
  }

  def convert(plan: Plan): LogicalPlan = {
    val root = plan.getRoots.get(0)
    val names = root.getNames.asScala
    val output = names.map(name => AttributeReference(name, DataTypes.StringType)())
    withOutput(output) {
      val logicalPlan = root.getInput.accept(this);
      val projectList: List[NamedExpression] = logicalPlan.output.zipWithIndex
        .map(
          z => {
            val (e, i) = z;
            if (e.name == names(i)) {
              e
            } else {
              Alias(e, names(i))()
            }
          })
        .toList
      val wrapper = Project(projectList, logicalPlan)
      require(wrapper.resolved)
      wrapper
    }
  }
}
