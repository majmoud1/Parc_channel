package com.sonatel.parc.datastream

import com.sonatel.parc.session.CreateSession
import org.apache.spark.sql.functions.lit

object Channel extends App {

  /*
    Function used to give all active customers by service in the parc OM
   */
  def getActiveCustomersChannel (dir : String, dateStart : String) = {
    val spark = CreateSession.spark
    import spark.implicits._

    val allParcs = Parc.getAllParcs()
    val omChannels = LoadFile.load("src/main/resources/source/AllUsers_20200322.csv", "|")
    val listUsers = omChannels.select("MSISDN").collect().map(_(0)).toList
    val parentFirstName = omChannels.select("PARENT_FIRST_NAME").collect().map(_(0)).toList
    val parentLastName = omChannels.select("PARENT_LAST_NAME").collect().map(_(0)).toList
    val parentUsersMSISDN = omChannels.select("PARENT_USER_MSISDN").collect().map(_(0)).toList
    val UsersInParcs = Parc.getUsersInParcs(dir, dateStart).select("msisdn", "transaction_tag", "ratio")

    var dfTotal =  Seq.empty[(String, String, Integer, String, String, Integer)]
      .toDF("msisdn", "transaction_tag", "ratio", "Parent_name", "Parent_msisdn", "parc_actif_service_jour")
    /*--------------------------------------------------------------------------------------------------------*/
    for (i <-0 to 3) {
      val msisdn = listUsers(i).toString
      val parentName = parentFirstName(i) + " " + parentLastName(i)
      val parentMsisdn = parentUsersMSISDN(i)
      var df = UsersInParcs.select("msisdn", "transaction_tag", "ratio").filter(UsersInParcs("msisdn") === s"$msisdn")
      df = df.withColumn("Parent_name", lit(parentName))
          .withColumn("Parent_msisdn", lit(parentMsisdn))
          .withColumn("parc_actif_service_jour", lit(1))
      // Select parc present in the dataFrame
      var parcsUser = df.select("transaction_tag").collect().map(_(0)).toList
      parcsUser = allParcs.diff(parcsUser)
      parcsUser.foreach(parc => {
        val row = Seq((s"$msisdn", s"$parc", 0, s"$parentName", s"$parentMsisdn", 0))
          .toDF("msisdn", "transaction_tag", "ratio", "Parent_name", "Parent_msisdn", "parc_actif_service_jour")
        df = row.union(df)
        df.show()
      })
      dfTotal = df.union(dfTotal)
    }
    //dfTotal = dfTotal.orderBy("msisdn")
    dfTotal.coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .csv("src/main/resources/CustomersChannel")
  }
  getActiveCustomersChannel("src/main/resources/parc2", "2020-02-01")
}
