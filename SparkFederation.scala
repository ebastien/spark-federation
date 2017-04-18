package name.ebastien.spark

/**
  * Demonstrate a custom Catalyst transformation to push filtered
  * aggregations down to a remote data source.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.execution.{RDDConversions, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.plans._

import org.http4s.{Uri, Method, Request, GenericCredentials, Headers, Header}
import org.http4s.client.blaze.PooledHttp1Client
import org.http4s.circe.jsonOf
import io.circe.generic.auto._

import scalaz.concurrent.Task

/**
  * A partition for our federated RDD
  *
  * @param idx the partition index
  */
case class FederatedPartition(idx: Int) extends Partition {
  override def index: Int = idx
}

/**
  * A RDD able to fetch rows from a remote source
  *
  * @param params a placeholder for proper parameters
  * @param sc
  */
class FederatedRDD(
  val params: String
)(@transient val sc: SparkContext) extends RDD[Row](sc, Nil) {

  // There is only one (dummy) partition
  val partition = FederatedPartition(0)

  // The HTTP client is instantiated upon request from the executor
  lazy val client = PooledHttp1Client()

  // Geonames API parameters
  val geonamesUser = "demo"
  val geonamesICAO = "LSZH"

  // The representation of weather information as case classes
  case class WeatherObservation(
    humidity: Int,
    elevation: Long
  )
  case class Representation(weatherObservation: WeatherObservation)

  // Compile a task for fetching weather information from Geonames
  def task: Task[Representation] = {
    // TODO: Make use of the RDD parameters somehow
    val req = Request(
      Method.GET,
      Uri.uri("http://api.geonames.org")
         .withPath("/weatherIcaoJSON")
         .withQueryParam("ICAO", geonamesICAO)
         .withQueryParam("username", geonamesUser)
    )
    client.expect(req)(jsonOf[Representation])
  }

  // Compute a partition from within a task on a executor
  override def compute(
    split: Partition,
    context: TaskContext
  ): Iterator[Row] = {
    // Would the HTTP task fail an exception is thrown
    val rep = task.unsafePerformSyncFor(5000)
    // Generate a rows iterator from the weather information
    Iterator(
      Row(
        rep.weatherObservation.humidity,
        rep.weatherObservation.elevation
      )
    )
  }

  override def getPartitions: Array[Partition] = Array(partition)
}

/**
  * A scanner interface to push-down filter and aggregation
  */
trait FederatedScan {

  /**
    * Build a RDD from a filtered aggregation
    *
    * @param filter
    * @param grouping
    * @param aggregate
    * @return
    */
  def buildScan(
    filter: Expression,
    grouping: Seq[Expression],
    aggregate: Seq[NamedExpression]
  ): RDD[Row]
}

/**
  * A concete relation implementing the push-down scanner
  *
  * @param sparkSession
  */
class FederatedRelation(@transient val sparkSession: SparkSession)
  extends BaseRelation with FederatedScan {

  // Our schema is a single field of type integer
  override def schema: StructType = {
    StructType(StructField("f1", IntegerType, true) :: Nil)
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def buildScan(
    filter: Expression,
    grouping: Seq[Expression],
    aggregate: Seq[NamedExpression]
  ): RDD[Row] = {

    // The demonstration consists in injecting the expressions
    // as a string down to the RDD.
    val params = s"[$filter] [${grouping.mkString(",")}] [${aggregate.mkString(",")}]"

    new FederatedRDD(params)(sqlContext.sparkContext)
  }
}

/**
  * Build a federated relation for DataFrames with the matching format
  */
class FederatedRelationProvider
  extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "federated"

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String,String]
  ): BaseRelation = {
    new FederatedRelation(sqlContext.sparkSession)
  }
}

/**
  * Strategy allowing push-down of filtered aggregations
  */
object FederatedStrategy extends Strategy {
  def apply(plan: logical.LogicalPlan): Seq[SparkPlan] = plan match {
      // Would likely benefit from a PredicateHelper like in
      // org.apache.spark.sql.catalyst.planning
    case l @
      logical.Aggregate(grouping, aggregate,
        logical.Filter(filter,
          LogicalRelation(baseRelation: FederatedScan, _, _))) =>

      val outAttributes: Seq[Attribute] = l.output

      val outTypes: Seq[DataType] = outAttributes.map(_.dataType)

      // We capture the filter and aggregation expressions in our RDD
      val rdd: RDD[Row] = baseRelation.buildScan(filter, grouping, aggregate)

      val internalRDD: RDD[InternalRow] = RDDConversions.rowToRowRdd(rdd, outTypes)

      // Generate the actual physical plan from the RDD
      // and the expected attributes and types.
      // It would only explode at runtime would some types do not match...
      RowDataSourceScanExec(
        output = outAttributes,
        rdd = internalRDD,
        relation = baseRelation,
        outputPartitioning = physical.UnknownPartitioning(0),
        metadata = Map.empty,
        metastoreTableIdentifier = None
      ) :: Nil
    case _ => Nil
  }
}

/**
  * Our Spark application
  */
object SparkFederation {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL federation")
      .master("local")
      .getOrCreate()

    spark.experimental.extraStrategies = Seq(FederatedStrategy)

    spark.read
         .format("name.ebastien.spark.FederatedRelationProvider")
         .load
         .createOrReplaceTempView("fed")

    val df = spark.sql(
      "SELECT f1, count(*) FROM fed WHERE f1 >= 42 GROUP BY f1"
    )

    df.explain(true)
    df.show()

    spark.stop
  }
}
