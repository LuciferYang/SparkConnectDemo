package personal.code

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf

object ConnectDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .client(SparkConnectClient.builder().port(15102).build()).build()
    try {
      def dummyUdf(x: Int): Int = x + 5

      val myUdf = udf(dummyUdf _)
      val df = spark.range(5).select(myUdf(Column("id")))

      val result = df.collectResult()
      println(result.length)
      result.toArray.zipWithIndex.foreach { case (v, idx) =>
        println(v.getInt(0))
      }
    } finally {
      spark.close()
    }
  }
}
