import scala.math.exp

trait TopoFactor extends Serializable {
  def gen(maxIter:Int, currentIter:Int, nbNeurons: Int): Array[Double]
}

/** 
  Calcul du voisinage avec la formule donnée. 
   T représente la température qui diminue graduellement en fonction  de Tmax et Tmin,
    afin de contrôler la taille du voisinage qui influence une cellule donnée sur la carte 
*/
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
    Array.tabulate(nbNeurons*2)(i => if (i == 0) exp(i / T) else Double.MaxValue)
  }
}

