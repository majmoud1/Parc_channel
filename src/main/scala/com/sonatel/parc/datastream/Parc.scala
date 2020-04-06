package com.sonatel.parc.datastream

import com.sonatel.parc.session.CreateSession
import org.apache.spark.sql.functions.date_sub
import org.apache.spark.sql.functions.lit

import scala.collection.mutable.ListBuffer


object Parc {

  // Define all properties
  //System.setProperty("hadoop.home.dir", "C:\\winutils\\")


  val spark = CreateSession.spark
  /*
    Function used to load the Json file and generate the sql request and the regroupement from a parc
  */
  def getRequestAndRegroupement(dir : String, dateStart : String) : List[String]  = {
    // Recover started date
    import spark.implicits._
    val dateEnd = Seq(s"$dateStart").toDF("date")
      .select($"date", date_sub($"date", 30).as("diff"))
    val elementsFirst = dateStart.split("-")
    val yearFirst = elementsFirst(0)
    val monthFirst = elementsFirst(1)
    val dFirst = elementsFirst(2)
    val dayFirst = s"$yearFirst$monthFirst$dFirst".toInt

    val elementsEnd = dateEnd.select($"diff").first().get(0).toString.split("-")
    val yearEnd = elementsEnd(0)
    val monthEnd = elementsEnd(1)
    val dEnd = elementsEnd(2)
    val dayEnd = s"$yearEnd$monthEnd$dEnd".toInt

    var requests = new ListBuffer[String]()
    val files = SelectFilesParcs.getFiles(dir)
    files.foreach(file => {
      val lRule = Functions.getRuleObject(file.toString)
      val parcRule = spark.sparkContext.broadcast(lRule)
      // Recover the column selected and the wording
      val colparc = parcRule.value.colparc.trim
      val regroupement = parcRule.value.regroupement.trim
      var andFilter = ""
      // This dico is used to know the ratio of users by service (transaction_tag)
      val dico = Map("sender_msisdn" -> "receiver_msisdn", "receiver_msisdn" -> "sender_msisdn")
      val sens = dico(colparc)
      var request =
        s"""
           |SELECT $colparc AS msisdn, $sens FROM transactions WHERE
           |""".stripMargin
      if (parcRule.value.filters.length > 0) { andFilter = " AND " }
      for (level1 <- parcRule.value.filters) {
        for (level2 <- level1.value) {
          val colSelected = level2.col
          val operator = level2.operator
          // Verified if the number of values is equal to 1
          var value2 = " "
          if (level2.value.length == 1) {
            val value1 = level2.value(0)
            request = request + colSelected + operator + s"'$value1'" + s" $andFilter "
          }
          // Recover all values
          else {
            value2 = value2 + " ("
            for (v <- level2.value) { value2 = value2 + s"'$v'" + "," }
            request = request + colSelected + s" $operator " + value2.substring(0, value2.length - 1) + ")" + s" $andFilter "
          }
        }
      }
      request = request +
        s"""
           |year BETWEEN $yearEnd AND $yearFirst AND
           |month BETWEEN $monthEnd AND $monthFirst AND
           |day BETWEEN $dayEnd AND $dayFirst ;$regroupement;$sens""".stripMargin

      requests += request
    })
    requests.toList
  }

  /*
    Function used to select all users finding in all parcs
   */
  def getUsersInParcs(dir : String, dateStart : String)  = {

    val spark = CreateSession.spark
    // Create a dataframe. This dataframe will be used to merge all results
    import spark.implicits._
    var dfMerge =  Seq.empty[(String, String, Integer)].toDF("msisdn", "transaction_tag", "ratio")

    val fileSource = LoadFile.load("src/main/resources/source/transactions_om_2020.csv")
    fileSource.createOrReplaceTempView("transactions")
    val requests = getRequestAndRegroupement(dir, dateStart)
    requests.foreach(req => {
      val list = req.split(";")
      val request = list(0)
      val regroupement = list(1)
      val sens = list(2)
      val df = spark.sql(s"$request")
        .dropDuplicates()
        .drop(s"$sens")
        .withColumn("transaction_tag", lit(regroupement))
        .groupBy("msisdn", "transaction_tag")
        .count()
        .withColumnRenamed("count", "ratio")
      dfMerge = df.union(dfMerge)
      //dfMerge.show()
    })
    dfMerge
  }
  //getUsersInParcs("src/main/resources/parcs", "2020-02-01").show(500)

  /*
    Function used to get all parcs names
   */
  def getAllParcs(path: String = "src/main/resources/parcs") : List[String] = {
    val filesJson = SelectFilesParcs.getFiles(s"$path")
    var parcs = new ListBuffer[String]()
    if(filesJson.nonEmpty) {
      filesJson.foreach(file => {
        val lRule = Functions.getRuleObject(s"$file")
        val parcRule = spark.sparkContext.broadcast(lRule)
        parcs += parcRule.value.regroupement
      })
    }
    else
    {
      println("Directory empty or not exist!!!")
    }
    parcs.toList
  }

}
