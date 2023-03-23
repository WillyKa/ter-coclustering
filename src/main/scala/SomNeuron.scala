import org.apache.spark.ml.linalg.Vector
import scala.math.{abs, exp}

class SomNeuron(id: Int, val row: Int, val col: Int, point: Vector) extends Serializable {
  def factorDist(neuron: SomNeuron, T: Double): Double = {
    exp(-(abs(neuron.row - row) + abs(neuron.col - col)) / T)
  }

  override def toString: String = {
    "(" + row + ", " + col + ") -> " + point
  }
}