import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object practicewhen {

  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","first-program")
    sparkconf.set("spark.master","local[*]")


    val spark=SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()



    //    val schema=" id Int,name String,age Int"

    val schema=StructType(List(

      StructField("id", IntegerType),
      StructField("product", StringType),
      StructField("price", IntegerType),
      StructField("quantity", IntegerType)
    ))

//    val df=spark.read
//      .format("csv")
//      .option("header",true)
//      .schema(schema)
//      .option("path","C:/Users/Amit/Desktop/lt.csv")
//      .load()
//
//
//    df.select(col("id"),col("name"),col("age"),when(col("age")>18 && col("age")<=30,"young")
//      .when(col("age")>30 && col("age")<=50,"old").otherwise("pensionbale")).show()

    //
//        import spark.implicits._
//
//
//
////        val df= List(("karthik",89,"mum"),("mohan",78,"kerala")).toDF("name","age","city")
////
////
////        df.select((col("age")).alias("sumofage")).show()
////
////        df.select((col("name")).alias("countofname")).show()
//
//
////        val df = List(
////          ("Order1", "John", 100),
////          ("Order2", "Alice", 200),
////          ("Order3", "Bob", 150),
////          ("Order4", "Alice", 300),
////          ("Order5", "Bob", 250),
////          ("Order6", "John", 400)
////        ).toDF("OrderID", "Customer", "Amount")
////
////
////
////        df.groupBy("Customer","Amount").agg(count("OrderID").alias("count_update")).show()
////
//import org.apache.spark.sql.functions._
//        val salesData = List(
//          ("Product1", "Category1", 100),
//          ("Product2", "Category2", 200),
//          ("Product3", "Category1", 150),
//          ("Product4", "Category3", 300),
//          ("Product5", "Category2", 250),
//          ("Product6", "Category3", 180)
//        ).toDF("Product", "Category", "Revenue")
//
//        val windowSpec = Window.partitionBy("Category")
//        val df = salesData.withColumn("max_revenue", max("Revenue").over(windowSpec))
//        val df1 = df
//        df1.select("product","max_revenue").show()
//



    //  val schema="id Int,name String,age Int"

//        val schema=StructType(List(
//
//          StructField("id",IntegerType),
//            StructField("product",StringType),
//            StructField("price",IntegerType),
//          StructField("quantity",IntegerType)
//
//        ))


        val df=spark.read
          .format("csv")
          .option("header",true)
          .schema(schema)
          .option("mode","DROPMALFORMED")
          .option("path","C:/Users/Amit/Desktop/lt.csv")
          .load()

//        df.show()
        val windowSpec = Window.partitionBy("product").orderBy("quantity")
        val df1 = df.withColumn("total_avg", count("price").over(windowSpec))
        df1.show()

    //    import spark.implicits._


    //   val mylist= List(("karthik kondpak",89,"mum"),("mohan sharma",78,"kerala"))
    //
    //    val data=spark.createDataFrame(mylist)
    //
    //    val df2=data.toDF("name","age","city")
    //
    //   val df3= df2.select(initcap(column("name"))).show()


    //    val df = List(("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Alice", 67)).toDF("name", "age")
    //
    //
    //    val newc=df.withColumn("pensionable",when(col("age")>55,"yes").otherwise("No"))
    //
    //    val renamedf=df.withColumnRenamed("name","fullname")
    ////    renamedf.show()
    //
    //    val newc1=df.select(col("age"),when(col("age")>55,"yes").otherwise("No").alias("pensionable"))
    //
    //
    //    val fd=df.filter(col("age")>55 || col("name").startsWith("A"))
    //
    //    val fd1=df.filter(col("age")>55 && col("name").endsWith("e"))
    //
    //
    //
    //    newc.show()
    //
    //    newc1.show()










    scala.io.StdIn.readInt()

  }

}