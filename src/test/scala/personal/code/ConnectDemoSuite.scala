package personal.code

import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf

class ConnectDemoSuite extends FunSuite with Serializable {

  test("simple udf test") {
    val spark = SparkSession.builder()
      .client(SparkConnectClient.builder().port(15102).build()).build()
    try {

      def dummyUdf(x: Int): Int = x + 5
      val myUdf = udf(dummyUdf _)
      val df = spark.range(5).select(myUdf(Column("id")))

      val result = df.collectResult()
      assert(result.length == 5)
      result.toArray.zipWithIndex.foreach { case (v, idx) =>
        assert(v.getInt(0) == idx + 5)
      }
    } finally {
      spark.close()
    }
  }
}
