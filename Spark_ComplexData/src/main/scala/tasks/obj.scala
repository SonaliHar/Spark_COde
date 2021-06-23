package tasks


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

import org.apache.spark.sql.Row


import scala.io.Source

import scala.util.matching.Regex

import  org.apache.spark.sql.functions.regexp_replace
  

object obj {
  
  def main(args:Array[String]):Unit={
    
      val conf=new SparkConf().setAppName("Batch28").setMaster("local[*]")
      val sc=new SparkContext(conf)
      sc.setLogLevel("Error")
      
      val spark= SparkSession.builder().getOrCreate()
      
      
     import  spark.implicits._  
   
     println("******************************Url Data***************************************")  
 
      
    val apidata=Source.fromURL("https://randomuser.me/api/0.8/?results=1000")
      
      val apidata1= apidata.mkString
      
      val urlData2= sc.parallelize(List(apidata1))
      
     
     val resultDF = spark.read.json(urlData2)

      
      resultDF.printSchema()
      
      
      
     val arraydf= resultDF.withColumn("results",explode(col("results"))) 
      
      
      arraydf.show(3)
      arraydf.printSchema()
      
      
    val  flattendf=arraydf.
    select(col("nationality"),
          // col("results.user.DNI").alias("DNI"),
           col("results.user.cell").alias("cell"),
           col("results.user.dob").alias("dob"),
           col("results.user.email").alias("email"),
           col("results.user.gender").alias("gender"),
           col("results.user.location.city").alias("city"),
           col("results.user.location.state").alias("state"),
           col("results.user.location.street").alias("street"),
           col("results.user.location.zip").alias("zip"),
           col("results.user.md5").alias("md5"),
           col("results.user.name.first").alias("first"),
           col("results.user.name.last").alias("last"),
           col("results.user.name.title").alias("title"),
           col("results.user.password").alias("password"),
           col("results.user.phone").alias("phone"),
           col("results.user.picture.large").alias("large"),
           col("results.user.picture.medium").alias("medium"),
           col("results.user.picture.thumbnail").alias("thumbnail"),
           col("results.user.registered").alias("registered"),
           col("results.user.salt").alias("salt"),
            col("results.user.sha1").alias("sha1"),
           col("results.user.sha256").alias("sha256"),
           col("results.user.username").alias("username"),
           col("seed").alias("seed"),
           col("version").alias("version")
  
    )
      
      
            
     // flattendf.show(3)
   //   flattendf.printSchema()
      
      
  val complexdf= flattendf.select(
          col("nationality"),
        struct(
             struct(
               //  col("AVS"),
                 col("cell"),
                 col("dob"),
                 col("email"),
                 col("gender"),
                 struct(
                   col("city"),
                   col("state"),
                   col("street"),
                   col("zip")).alias("location"),
                col("md5"),
               struct(
                 col("first"),
                 col("last"),
                 col("title")
               ).alias("name"),
               col("password"),
               col("phone"),
               struct(
               col("large"),
               col("medium"),
               col("thumbnail")
               ).alias("picture"),
            col("registered"),
            col("salt"),
            col("sha1"),
            col("sha256"),
            col("username")
                 ).alias("user")
                 ).alias("results"),
         col("seed"),
         col("version")
).groupBy(col("nationality"),col("seed"),col("version")).agg(collect_list("results").alias("results"))
      
      
    
complexdf.printSchema()
   
    }
  
  
}