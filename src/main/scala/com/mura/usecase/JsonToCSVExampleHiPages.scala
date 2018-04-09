package com.mura.usecase

import org.json4s._
import org.json4s.JsonDSL._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions._

import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date

/**
  * @author Muralidhar Rada
  * @since 07/04/18.
  */
object JsonToCSVExampleHiPages {
  
  def dateConversion(inputDate:String) = {
val originFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
val targetFormat = new SimpleDateFormat("yyyyMMddHH")
val output = targetFormat.format(originFormat.parse(inputDate))
output
}
  
  def main(args: Array[String]){
    
   val spark = SparkSession.builder()
      .appName("First Spark application - reading json")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    
    var inputPath = "file:///C://Users//pc//Desktop//Hipages//source_event_data.json"
    var outputPath1 = "file:///C://Users//pc//Desktop//Hipages//firstresult.csv"
    var outputPath2 = "file:///C://Users//pc//Desktop//Hipages//secondresult.csv"
    
    val rdd = sc.textFile(inputPath)

    val parsedRec =  rdd.map{record =>
        try {
          implicit val formats = DefaultFormats
          val json = parse(record)
          
          val event_id = (json \ "event_id").extract[String]  
          val session_id = ((json \ "user") \ "session_id").extract[String]
          val id = ((json \ "user") \ "id").extract[String]
          val ip = ((json \ "user") \ "ip").extract[String]
          val action = (json \ "action").extract[String]
          val url = (json \ "url").extract[String]
          val url_split=  url.substring(url.indexOf(".") + 1).split("/")
    
          val url_level1 = url_split(0)
          val url_level2 = url_split(1)
          val url_level3 = url_split.lift(2).getOrElse("null")
          val timestamp = (json \ "timestamp").extract[String]
          val timestamp1 = (json \ "timestamp").extract[String]
          val timestamp_tbl2 = dateConversion(timestamp1)
   
          (session_id,timestamp,timestamp_tbl2,url_level1,url_level2,url_level3,action) 			  
  }
    catch {
          case t: Throwable => {
            throw new RuntimeException("Parsing error: " + t.getMessage())
          }
        }
  }

//   parsedRec.take(3).foreach(println)
   
  
   import spark.implicits._
   
   val df=parsedRec.toDF("user_id","time_stamp","timestamp_tbl2","url_level1","url_level2","url_level3","activity")

   df.registerTempTable("SourceTable")

   val sql1 = spark.sql("""select user_id
                          , time_stamp
                          , url_level1
                          , url_level2
                          , url_level3
                          , activity 
               from SourceTable""")
   
              
   sql1.write
   .option("header","true")
   .csv(outputPath1)
 
// We can use the below code to write entire output into a single partition.
//   sql1.repartition(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(outputPath)
   
//   val res = spark.sql("""select * from SourceTable""").map(x=>x.toString()).rdd

//   res.coalesce(1).saveAsTextFile("out.csv")
   
   val sql2 = spark.sql("""select timestamp_tbl2 as time_bucket
                        , url_level1,url_level2
                        ,activity
                        ,count(activity) as activity_count
                        ,count(distinct user_id) as user_count
               from SourceTable 
               group by timestamp_tbl2,activity,url_level1,url_level2
               """)
    sql1.write
   .option("header","true")
   .csv(outputPath2)
   
   spark.stop()

  }
}