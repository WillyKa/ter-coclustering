import scala.math.exp

/**
  * Company : Altic - LIPN
  * User: Tugdual Sarazin
  * Date: 06/01/14
  * Time: 12:08
  */
trait TopoFactor extends Serializable {
  def gen(maxIter:Int, currentIter:Int, nbNeurons: Int): Array[Double]
}

object BiTMTopoFactor extends TopoFactor {
  def gen(maxIter:Int, currentIter:Int, nbNeurons: Int) = {
    val Tmin:Double = 0.9
    val Tmax:Double = 8
    val T:Double =Math.pow(Tmax * (Tmin /Tmax), (currentIter/maxIter.toDouble))
    
    Array.tabulate(nbNeurons*2)(dist => exp(-dist / T))
  }
}

object CroeucTopoFactor extends TopoFactor {
  def gen(maxIter: Int, currentIter: Int, nbNeurons: Int): Array[Double] = {
    val T:Double = (maxIter - currentIter + 1) / maxIter.toDouble
    //val T = 0.9
    Array.tabulate(nbNeurons*2)(i => if (i == 0) exp(i / T) else Double.MaxValue)
  }
}