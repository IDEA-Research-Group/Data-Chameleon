package es.us.idea.adt.data.chameleon.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.mutable

//object utils {
//  def fromRowToMap(row: Row, dsSchema: Option[StructType] = None):Map[String, Any] = {
//    if(dsSchema.isDefined) format(dsSchema.get, new GenericRowWithSchema(row.toSeq.toArray, dsSchema.get) ) else format(row.schema, row)
//  }
//
//  def format(schema: Seq[StructField], row: Row): Map[String, Any] = {
//    var result:Map[String, Any] = Map()
//
//    schema.foreach(s => //println(s.dataType)
//      s.dataType match {
//        case ArrayType(elementType, _)=> val thisRow = row.getAs[mutable.WrappedArray[Any]](s.name); result = result ++ Map(s.name -> formatArray(elementType, thisRow))
//        case StructType(structFields)=> val thisRow = row.getAs[Row](s.name); result = result ++ Map( s.name -> format(thisRow.schema, thisRow))
//        case _ => result = result ++ Map(s.name -> row.getAs(s.name))
//      }
//    )
//    return result
//  }
//
//  def formatArray(elementType: DataType, array: mutable.WrappedArray[Any]): Seq[Any] = {
//    elementType match {
//      case StructType(structFields) => array.map(e => format(structFields, e.asInstanceOf[Row]))
//      case ArrayType(elementType2, _) => array.map(e => formatArray(elementType2, e.asInstanceOf[mutable.WrappedArray[Any]]))
//      case _ => array
//    }
//  }
//
//  def findMinSetOfPaths(inPaths: Seq[String]) = {
//
//    val inPathsSplit = inPaths.filter(_.nonEmpty).map(_.split('.'))
//
//    inPathsSplit.map(s => {
//      if(s.length > 0) s.head
//      else s.mkString(".")
//    }).distinct
//
//  }
//
//}
