package name.ebastien.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext, Partition}
import org.apache.spark.sql.{SQLContext, SparkSession, SaveMode, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.sources.{BaseRelation,
                                     RelationProvider,
                                     DataSourceRegister,
                                     PrunedFilteredScan,
                                     Filter}

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

class FederatedRelation(@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan {

  override def schema: StructType = {
    StructType(StructField("f1", IntegerType, true) :: Nil)
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def buildScan(
    requiredColumns: Array[String],
    filters: Array[Filter]
  ): RDD[Row] = new FederatedRDD(sqlContext.sparkContext)
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

object SparkFederation {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL federation")
      .master("local")
      .getOrCreate()

    spark.read
         .format("name.ebastien.spark.FederatedRelationProvider")
         .load
         .createOrReplaceTempView("fed")

    spark.sql("SELECT * FROM fed").show()

    spark.stop
  }
}
