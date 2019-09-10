package es.us.idea.adt.data.chameleon.dsl
import es.us.idea.adt.data.chameleon.{Index, IndexNested, Select, SelectNested}
import es.us.idea.adt.data.chameleon.internal.Evaluable

import scala.util.Try

class SelectContainer(path: String) extends ExpressionContainer {
  override def build(): Evaluable = interpretPath(path.split('.'))

  def interpretPath(path: Array[String]): Evaluable = {
    if(path.length == 0) new Select()
    else if(path.length == 1) {
      val subpath = path.head
      if(subpath == "") new Select()
      else if(pathPointsToArray(subpath)){
        new Index(getIndexFromPath(subpath))
      } else {
        new Select(subpath)
      }
    }
    else {
      val root = path.head
      if(pathPointsToArray(root)) {
        new IndexNested(getIndexFromPath(root), interpretPath(path.tail))
      } else {
        new SelectNested(root, interpretPath(path.tail))
      }
    }
  }

  def pathPointsToArray(path: String): Boolean = {
    path.matches("^\\[\\d+\\]$")
  }

  def getIndexFromPath(path: String): Int = {
    Try(path.replaceAllLiterally("[", "").replaceAllLiterally("]", "").toInt)
      .toOption match {
      case Some(i: Int) => i
      case _ => throw new Exception(s"Error interpreting subpath $path. It was expected to be an index.")
    }
  }

}
