package com.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
object DF {
  com.spark.DF
  System.setProperty("java.security.krb5.conf", "C:\\Users\\Kanhu\\Desktop\\krb5.ini");
  System.setProperty("sun.security.krb5.realm", "C:\\Users\\Kanhu\\Desktop\\krb5.ini");
  //java.security.krb5.config=C:\\Users\\Kanhu\\Desktop\\krb5.ini
  def main(args: Array[String]) {
  val conf = new SparkConf().setAppName("DF_join").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val payment = sc.parallelize(Seq((1, 101,2500,'Y'), (2,102,1110,'N'), (3,103,500), (4 ,104,400,'Y'), (5 ,105, 150,'N'), (6 ,106, 450,'Y')))
  .toDF("paymentId", "customerId","amount","is_act")
  val customer = sc.parallelize(Seq((101,"Jon") , (102,"Aron") ,(103,"Sam"))).toDF("customerId", "name")
  //payment.show()
  //You DF names above and also in join statement below:
  //var leftJoinDf = payment.join(customer,Seq("customerId"), "left")     //Second option
  var leftJoinDf= payment.join(customer,payment("customerId") ===  customer("customerId"),"right").show(false)                           
                     
  //OR
  
  payment.registerTempTable( "pay" )
  customer.registerTempTable( "Cust" )
  val  Final_DF=sqlContext.sql("""select Cust.*
  from Cust T1  
  left join pay T2 on T1.customerId = T2.customerId and T1.is_act = 'Y'""")
  Final_DF.write.mode("append").saveAsTable("FinaltableName")
  
  }
}