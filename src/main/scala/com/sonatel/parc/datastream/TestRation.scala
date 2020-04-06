package com.sonatel.parc.datastream

import com.sonatel.parc.session.CreateSession
import org.apache.spark.sql.functions.lit

object TestRation extends App {

  val spark = CreateSession.spark

  val fileSource = LoadFile.load("src/main/resources/source/transactions_om_2020.csv")
  fileSource.createOrReplaceTempView("transactions")

  /*val df = spark.sql(
    """
      |SELECT sender_msisdn AS msisdn, receiver_msisdn FROM transactions WHERE transfer_status='TS'  AND  transaction_tag='CASHIN'
      |AND  transfer_subtype='CASHIN'  AND year BETWEEN 2020 AND 2020 AND month BETWEEN 01 AND 02 AND day BETWEEN 20200102 AND 20200201
      |""".stripMargin)
    .dropDuplicates()
    .drop("receiver_msisdn")
    .withColumn("transaction_tag", lit("CASHIN"))
    .groupBy("msisdn", "transaction_tag")
    .count()
    .withColumnRenamed("count", "Ratio")*/
  val df = spark.sql(
    """
      |SELECT DISTINCT sender_msisdn AS msisdn, COUNT(DISTINCT receiver_msisdn) AS ratio FROM transactions WHERE transfer_status='TS'  AND  transaction_tag='CASHIN'
      |AND  transfer_subtype='CASHIN'  AND year BETWEEN 2020 AND 2020 AND month BETWEEN 01 AND 02 AND day BETWEEN 20200102 AND 20200201
      |GROUP BY sender_msisdn
      |""".stripMargin)
  df.show()

  // Test dico
  /*val dico = Map("sender_msisdn" -> "receiver_msisdn", "receiver_msisdn" -> "sender_msisdn")
  println(dico("receiver_msisdn"))*/

  /*val allRequests = Parc.getRequestAndRegroupement("src/main/resources/parcs", "2020-02-01")
  println(allRequests)*/

}
