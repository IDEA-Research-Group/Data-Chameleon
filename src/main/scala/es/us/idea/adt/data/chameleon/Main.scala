package es.us.idea.adt.data.chameleon

import es.us.idea.adt.data.chameleon.data.simple.{DateType, IntegerType}
import es.us.idea.adt.data.chameleon.internal.dtf.udf.UDF
import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]) = {
    //args.headOption match {
    //  case Some(s) => execute(s)
    //  case _ => println("Error: No dataset path specified.")
    //}
  }

  execute("datasets/power_consumption.json")

  def execute(datasetPath: String) = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark ADT")
      .getOrCreate()

    import es.us.idea.adt.data.chameleon.dsl.implicits._
    import es.us.idea.adt.data.chameleon.spark.implicits._
    import es.us.idea.adt.data.chameleon.internal.dtfs._

    val translations = Seq("TA", "TB", "TC", "TD", "TE", "TF", "TG", "TH", "TI", "TJ", "TK", "TL", "TM").zipWithIndex.toMap

    // ADT User Defined Function to translate the "tariff" field
    //val translate = ADTDataFunction((s: String) => {
    //  translations.getOrElse(s, -1)
    //} , DataTypes.IntegerType)

    // ADT User Defined Function to calculate the number of days between the "startDate" and "endDate"
    //val daysBetweenDates = ADTReductionFunction((s: Seq[Some[java.sql.Date]]) => {
    //  (s.headOption, s.lastOption) match {
    //    case (Some(endOpt), Some(startOpt)) => (endOpt, startOpt) match {
    //      case (Some(end), Some(start)) => Days.daysBetween(new LocalDate(start), new LocalDate(end)).getDays
    //      case _ => None
    //    }
    //    case _ => None
    //  }
    //} , DataTypes.IntegerType)

    val translate = UDF((s: String) => {
      0
    }, new IntegerType)

    // Read the Dataset and apply the Data Transformation Functions
    val ds = spark.read.json(datasetPath)
        .chameleon(
          "ID" << t"customerID",
          "T" << translate(t"tariff"),
          "CP" << struct (
            "p1" << array(t"contractedPower.period1", t"contractedPower.period4") -> max -> toInt,
            "p2" << array(t"contractedPower.period2", t"contractedPower.period5") -> max -> toInt,
            "p3" << array(t"contractedPower.period3", t"contractedPower.period6") -> max -> toInt
          ),
          "C" << (t"consumption" iterate array(
            array(t"power.period1", t"power.period4") -> max,
            array(t"power.period2", t"power.period5") -> max,
            array(t"power.period3", t"power.period6") -> max
          ))

        ).chameleon(
          "AVG_C" << struct(
            "p1" << (t"C" iterate t"[0]") -> avg,
            "p2" << (t"C" iterate t"[1]") -> avg,
            "p3" << (t"C" iterate t"[2]") -> avg
          )
        ).select("ID", "T", "CP", "C", "AVG_C")

    //"Group" << (t"consumption" -> groupBy(t"power.period1", t"." -> count) iterate struct(
    //  "potencia1" << t"__key",
    //  "count" << t"result"
    //))


    // Show a preview of the Dataset and print its schema
    ds.show()
    ds.printSchema()

    spark.close()
  }

}
