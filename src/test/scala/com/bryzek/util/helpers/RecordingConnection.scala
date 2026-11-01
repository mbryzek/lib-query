package helpers

import com.bryzek.util.Query

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, PreparedStatement}
import scala.collection.mutable

/** A JDBC connection that opens nothing and records what anorm sets on the statement it prepares, so a spec exercises
  * the placeholder filling a real execution performs, MissingParameter included.
  */
object RecordingConnection {

  /** The values anorm fills the query's placeholders with, in statement order. */
  def fill(query: Query): Seq[String] = {
    val values = mutable.TreeMap.empty[Int, String]

    val statement = proxy[PreparedStatement] { (method, args) =>
      if (method.getName.startsWith("set") && args.length >= 2) {
        args(0) match {
          case i: java.lang.Integer => values.put(i, String.valueOf(args(1)))
          case _ => ()
        }
      }
      ()
    }

    val connection = proxy[Connection] { (method, _) =>
      if (method.getName == "prepareStatement") statement else ()
    }

    query.anormSql().unsafeStatement(connection)
    values.values.toSeq
  }

  private def proxy[T](handle: (Method, Array[AnyRef]) => Any)(implicit tag: scala.reflect.ClassTag[T]): T = {
    val handler: InvocationHandler = (_, method, args) => {
      val result = handle(method, Option(args).getOrElse(Array.empty[AnyRef]))
      if (method.getReturnType == java.lang.Void.TYPE) null
      else
        result match {
          case () => default(method.getReturnType)
          case other => other.asInstanceOf[AnyRef]
        }
    }
    Proxy
      .newProxyInstance(tag.runtimeClass.getClassLoader, Array(tag.runtimeClass), handler)
      .asInstanceOf[T]
  }

  private def default(returnType: Class[?]): AnyRef = {
    if (returnType == java.lang.Boolean.TYPE) java.lang.Boolean.FALSE
    else if (returnType == java.lang.Integer.TYPE) Integer.valueOf(0)
    else if (returnType == java.lang.Long.TYPE) java.lang.Long.valueOf(0L)
    else null
  }
}
