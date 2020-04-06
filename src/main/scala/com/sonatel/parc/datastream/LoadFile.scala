package com.sonatel.parc.datastream

import com.sonatel.parc.session.CreateSession
import org.apache.spark.sql

object LoadFile {

  val spark = CreateSession.spark

  /*
    Function used to load the file source
   */
  def load(path: String, delimiter : String = ",") : sql.DataFrame  = {
    val file = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", s"$delimiter")
      .csv(s"$path")
    file
  }
}
