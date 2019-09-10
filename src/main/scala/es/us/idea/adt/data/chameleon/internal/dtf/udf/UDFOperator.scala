package es.us.idea.adt.data.chameleon.internal.dtf.udf

import es.us.idea.adt.data.chameleon.data.DataType
import es.us.idea.adt.data.chameleon.internal.Evaluable
import es.us.idea.adt.data.chameleon.internal.dtf.DTFOperator

import scala.util.Try

class UDFOperator(evals: Seq[Evaluable], udf: UDF) extends DTFOperator{

  override var dataType: Option[DataType] = None

  // FIXME: Added try to avoid problems with Null values. It Must be fixed!!!
  override def getValue(in: Any): Any = {
    Try(udf.evaluate(evals.map(_.getValue(in)): _*)).toOption
  }

  override def evaluate(parentDataType: DataType): DataType = {
    val dt = udf.getDataType
    this.dataType = Some(dt)
    dt
  }
}
