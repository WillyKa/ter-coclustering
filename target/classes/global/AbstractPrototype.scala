package global

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors.sqdist

/**
 * Created with IntelliJ IDEA.
 * User: tug
 * Date: 14/06/13
 * Time: 12:42
 * To change this template use File | Settings | File Templates.
 */
abstract class AbstractPrototype(val id: Int, var _point: Vector) extends Serializable {
  def update(newPoint: Vector): Double = {
    val dist = sqdist(newPoint,_point)
    _point = newPoint
    dist
  }

  def dist(data: Vector) = sqdist(data,_point)

  def dist(prototype: AbstractPrototype) = sqdist(prototype._point,_point)
}
