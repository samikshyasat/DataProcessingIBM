package learning.spark.dataframe
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit, regexp_replace, udf, when}
object DataProcessingWf extends Serializable{

  def main(args: Array[String]): Unit = {
    case class EmpSchema(Name: String, Gender: String,Department: String, Salary: String,Loc:String,Rating:String,Country:String)
    //creating spark session for local mode
    val spark= SparkSession.
                builder().
                appName("HelloSpark").
                master("local[3]").getOrCreate()
    //Step 2
    //read emp-data csv file from ibm cloud

     val inputDF = spark.read
      .format("csv")
      .option("header", true)
       .load("cos://jp-tok/cloud-object-storage-gw-cos-standard-geb-emp/undefinedemp-data.csv")



// write csv file data to DB2 as mentioned
    inputDF.write
      .format("jdbc")
      .option("url", "db2")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "#####")
      .save()


//Reading data from DB2
    val tableDf = spark.read
      .format("jdbc")
      .option("driver", "com.ibm.db2.jcc.DB2Driver")
      .option("url", "jdbc:db2://server1")
      .option("user", "joe")
      .option("password", "###")
      .option("dbtable", "myshcema.mytable")
      .load();

    import spark.implicits._
    val ds: Dataset[EmpSchema] = tableDf.as[EmpSchema]

//Read datafrom local
    val EmpDF= spark.read.
      option("header",true).option("inferschema",true)
      .csv("C:\\Users\\Samikshay\\Downloads\\emp-data.csv")

  //Data Cleaing Section

    //Removed Special characters from Salary column for further processing
    //converted Salary column from String to int for calculation after replacing all null value to 0
    val cleanedEmpDF = EmpDF.select(EmpDF.columns.map(c => regexp_replace(EmpDF(c), """[^A-Z a-z 0-9]""", "").alias(c)): _*)
    .na.fill(0,Array("salary")).withColumn("salary",col("salary").cast("int"))

    //Converted all "NULL" string to null and dropped null values from Department column as it make no sense
        val DeptDF =  cleanedEmpDF.withColumn("Department",
           when(col("Department")==="NULL" ,null)
             .otherwise(col("Department")))
        val CleanedDeptDF= DeptDF.na.drop(Seq("Department"))

    //Calculate avg salary of each dept
        val ResultDF=CleanedDeptDF.groupBy("Department").avg("salary")

        val cleanGenderDF=CleanedDeptDF.na.drop(Seq("Gender"))

    //Creating TempView to use spark sql
        cleanGenderDF.createOrReplaceTempView("emp")

    //calculate Gender ratio in each dept
        val ResultDF2 =
            spark.sql("select department,sum(case when Gender = 'Male' then 1 else 0 end)/count(*) as male_ratio, sum(case when Gender = 'Female' then 1 else 0 end)/count(*) as female_ratio from emp group by Department")

    //write both the dataframe to IBM cloudpath as parquet file
       ResultDF.write.format("parquet").save("cos://jp-tok/cloud-object-storage-gw-cos-standard-geb-emp/emp-data.parquet")
       ResultDF2.write.format("parquet").save("cos://jp-tok/cloud-object-storage-gw-cos-standard-geb-emp/emp-data.parquet")

//shutting down spark session
 spark.stop()


  }

}
