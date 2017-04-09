package name.ebastien.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext, Partition}
import org.apache.spark.sql.{SQLContext, SparkSession, SaveMode, Row, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.sources.{BaseRelation,
                                     RelationProvider,
                                     DataSourceRegister,
                                     PrunedFilteredScan,
                                     Filter}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{RowDataSourceScanExec,
                                       SparkPlan,
                                       RDDConversions}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning

case class FederatedPartition(idx: Int) extends Partition {
  override def index: Int = idx
}

class FederatedRDD(sc: SparkContext) extends RDD[Row](sc, Nil) {

  val partition = FederatedPartition(0)

  override def compute(
    split: Partition,
    context: TaskContext
  ): Iterator[Row] = Iterator(Row(42))

  override def getPartitions: Array[Partition] = Array(partition)
}

trait FederatedScan {
  def buildScan: RDD[Row]
}

class FederatedRelation(@transient val sparkSession: SparkSession)
  extends BaseRelation with FederatedScan {

  override def schema: StructType = {
    StructType(StructField("f1", IntegerType, true) :: Nil)
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def buildScan: RDD[Row] = new FederatedRDD(sqlContext.sparkContext)
}

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

object FederatedStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case l @ LogicalRelation(baseRelation: FederatedScan, _, _) =>
      RowDataSourceScanExec(
        l.output,
        RDDConversions.rowToRowRdd(
          baseRelation.buildScan,
          l.output.map(_.dataType)
        ),
        baseRelation,
        UnknownPartitioning(0),
        Map.empty,
        None) :: Nil
    case _ => Nil
  }
}

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
