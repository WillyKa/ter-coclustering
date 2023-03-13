import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.{Vector, Vectors}

import java.util.Random
import scala.{Int, Serializable}
import scala.math.abs
import scala.Predef._
import utils.{Matrix, NamedVector}

import scala.collection.mutable

// TODO : Assigner les lignes dans le calcul des colonnes (utilisation de RDD[Vector] à la place RDD[AffectedVector])
// TODO: Check if ArraySeq is the best container
class BiTM(val nbRowNeuron: Int, val nbColNeuron: Int, datas: RDD[Array[Double]], val topoFactor: TopoFactor) extends Serializable {
  def this(nbRowNeuron: Int, nbColNeuron: Int, datas: RDD[Array[Double]]) = this(nbRowNeuron, nbColNeuron, datas, BiTMTopoFactor)
  // model topo factor
  //protected val topoFactor = BiTMTopoFactor

  //

  private val _nbDataCol = datas.first().length
  private val _nbNeurons = nbRowNeuron * nbColNeuron

  // Init randomly column neurons affectation

  private val _rand = new Random()
  private var _colNeuronAffectation = Array.tabulate(_nbDataCol) { i => _rand.nextInt(_nbNeurons) }
  private var _neuronMatrix = initNeurons(_nbNeurons)


  val quantErrors = new mutable.Stack[Double]

  protected def initNeurons(nbNeurons: Int): Matrix = {

    val rand = new Random()
    val samples = datas.takeSample(withReplacement = false, nbNeurons, rand.nextInt())

    new Matrix(samples.map { samp =>
      Array.tabulate(nbNeurons) { i =>
        samp(rand.nextInt(samp.length))
      }
    })
  }


    def training(nbIter: Int) {
      //println("### Dataset ###")
      //datas.foreach(dataVec => println(dataVec.length+" : "+dataVec))

      //println("### Init - Neurons ###")
      //println(_neuronMatrix)

      // training
      for (iter <- 0 until nbIter) {

        // process quantification error
        //quantErrors.push(quantError())

        // Affectation des colonnes
        _colNeuronAffectation = new BiTMCol(_neuronMatrix, _nbNeurons, _nbDataCol, _colNeuronAffectation).computeColAffectation()
        println("MON AFF EST "+_colNeuronAffectation.length)



        // Affectation des lignes et re-calcul du modèle
        _neuronMatrix = new BiTMRow(_neuronMatrix, _nbNeurons, _colNeuronAffectation, nbIter, iter).computeNewNeurons

        //println("### "+iter+" - Neurons ###")
        //println(_neuronMatrix)
      }
    }

    class BiTMCol(neuronMatrix: Matrix, nbNeurons: Int, nbDataCol: Int, colNeuronAffectation: Array[Int]) extends Serializable {
      def computeColAffectation(): Array[Int] = {
        // Todo : reflechir a une integration par RDD process
        // Todo : retourner le bestRowNeuron dans cette partie
        //val distByColNeuron = datas.flatMap(localColDists)//.reduceByKey(_ + _)
        val distByColNeuron = datas.map(d => Vectors.dense(d)).flatMap(localColDists).reduceByKey(_ + _)
        //println("### Cols distances ###")
        //distByColNeuron.foreach(d => println("c"+(d._1/nbNeurons)+",n"+(d._1%nbNeurons)+" => "+d._2))

        // todo : regrouper reduceByKey.map
        val colsBestNeuron = distByColNeuron.map(d => mapToNeuronDist(d._1, d._2)).reduceByKey(minNeuronDist).map(d => (d._1, d._2.colNeuron)).collectAsMap()
        Array.tabulate(nbDataCol)(i => colsBestNeuron(i))
      }

      // Todo : update bestRowNeuron dans cette étape
      protected def localColDists(dataRow: Vector) = Array.tabulate(nbDataCol * nbNeurons) { i =>
        val neuronColId = i % nbNeurons
        val neuronRowId = findBestRowNeuron(dataRow)
        //val neuronRowId = 0
        val dataColId = i / nbNeurons
        val dataVal = dataRow(dataColId)
        val neuronVal = neuronMatrix(neuronRowId, neuronColId)
        val localDist = (neuronVal - dataVal) * (neuronVal - dataVal)
        //println("i"+i+" : c"+dataColId+";n"+neuronRowId+","+neuronColId+" => "+dataVal+";"+neuronVal+" => "+localDist)
        (i, localDist)
      }

      protected class NeuronDist(val colNeuron: Int, val dist: Double) extends Serializable {
        override def toString = "[" + colNeuron + "=" + dist + "]"
      }

      protected def minNeuronDist(n1: NeuronDist, n2: NeuronDist) = {
        if (n2.dist < n1.dist) n2
        else n1
      }

      protected def mapToNeuronDist(i: Int, dist: Double) = {
        val colNeuron = i % nbNeurons
        val dataColId = i / nbNeurons
        (dataColId, new NeuronDist(colNeuron, dist))
      }

      protected def findBestRowNeuron(rowData: Vector): Int = {
        // Find best row neuron
        var bestRowNeuron = Int.MaxValue
        var minDist = Double.MaxValue
        for (rowNeuronId <- 0 until nbRowNeuron) {
          val tmpDist = rowSquaredDistance(rowNeuronId, rowData)
          if (tmpDist < minDist) {
            minDist = tmpDist
            bestRowNeuron = rowNeuronId
          }
        }
        bestRowNeuron
      }

      protected def rowSquaredDistance(rowNeuronId: Int, rowData: Vector): Double = {
        var ans = 0.0
        for (colId <- rowData.toArray.indices) {
          val colNeuronId = colNeuronAffectation(colId)
          val valNeuron = neuronMatrix(rowNeuronId, colNeuronId)
          val valData = rowData(colId)
          ans += (valNeuron - valData) * (valNeuron - valData)
        }
        ans
      }
    }

    class BiTMRow(neuronMatrix: Matrix, nbNeurons: Int, colNeuronAffectation: Array[Int], maxIter: Int, currentIter: Int)
      extends Serializable {
      def computeNewNeurons: Matrix = {
        // Find best row neuron and generate observations
        //println("### Best neuron by row ###")

        val allObs = datas.map(d => Vectors.dense(d)).map(d => mapperRowAffectation(d))


        // Print observations
        /*val aa = allObs.collect()
      println("### Observations ###")
      aa.foreach{obs =>
        obs.foreach(obsRow => println(obsRow.mkString("|", " |", " |")))
        println("------------------------------------")
      }*/


        // generate new neurones values
        val sumAllObs = allObs.reduce(sumObs)
        new Matrix(sumAllObs.map(row => row.map(_.compute())))
      }

      // todo : faire une classe contenant toutes les ObsElem : Array[Array[ObsElem]]
      protected class ObsElem() extends Serializable {
        var value: Double = 0.0
        var sumFactor: Double = 0.0

        def add(dataElem: Double, factor: Double) {
          value += dataElem
          sumFactor += factor
        }

        def +=(obs: ObsElem) {
          this.value += obs.value
          this.sumFactor += obs.sumFactor
        }

        def compute(): Double = if (sumFactor > 0) value / sumFactor else 0

        override def toString: String = "%.2f".format(value) + "/" + "%.2f".format(sumFactor)
      }

      protected def mapperRowAffectation(dataRow: Vector): Array[Array[ObsElem]] = {
        // process best neuron for the dataRow
        val rowBestN = findBestRowNeuron(dataRow)
        //println("row"+dataRow+" : neuron"+rowBestN)

        // pre-process tological dist
        val topoFactors = topoFactor.gen(maxIter, currentIter, nbNeurons)
        //topoFactors.foreach(f => println("- "+f))

        // Process observations
        val obs = Array.fill(nbNeurons, nbNeurons)(new ObsElem())
        for (c <- 0 until dataRow.size) {
          val colBestN = colNeuronAffectation(c)
          val elem = dataRow(c)

          //obs(rowBestN)(colBestN).add(elem, 1.0)
          // parcours et met à jour tous les éléments de la map
          for (i <- 0 until nbNeurons; j <- 0 until nbNeurons) {
            val factor = topoFactors(abs(rowBestN - i) + abs(colBestN - j))
            obs(i)(j).add(elem / factor, 1 / factor)
          }
        }
        obs
      }

      protected def sumObs(obs1: Array[Array[ObsElem]], obs2: Array[Array[ObsElem]]): Array[Array[ObsElem]] = {
        for (i <- 0 until obs1.length) {
          for (j <- 0 until obs1(i).length) {
            obs1(i)(j) += obs2(i)(j)
          }
        }
        obs1
      }

      protected def findBestRowNeuron(rowData: Vector): Int = {
        // Find best row neuron
        var bestRowNeuron = Int.MaxValue
        var minDist = Double.MaxValue
        for (rowNeuronId <- 0 until nbRowNeuron) {
          val tmpDist = rowSquaredDistance(rowNeuronId, rowData)
          if (tmpDist < minDist) {
            minDist = tmpDist
            bestRowNeuron = rowNeuronId
          }
        }
        bestRowNeuron
      }

      protected def rowSquaredDistance(rowNeuronId: Int, rowData: Vector): Double = {
        var ans = 0.0
        for (colId <- rowData.toArray.indices) {
          val colNeuronId = colNeuronAffectation(colId)
          val valNeuron = neuronMatrix(rowNeuronId, colNeuronId)
          val valData = rowData(colId)
          ans += (valNeuron - valData) * (valNeuron - valData)
        }
        ans
      }
    }

  def affectation(toAffDatas: RDD[Array[Double]]): RDD[Array[Int]] = {
    val affData = toAffDatas.map((d => Vectors.dense(d))).map(rowData => (findBestRowNeuron(rowData), rowData))
    /*Pour chaque ligne du dataset on lui cherche le meilleur neurone ( findBestRowNeuron)  et on retourne le vecteur associé et le neurone associé*/

    val rowOrderData = affData.sortByKey().values
    /*On trie affData par les clés qui sont les indices des neuronnes auxquel ils sont affectés */



    rowOrderData.map(_.toArray.zipWithIndex.map(d => (_colNeuronAffectation(d._2), d._1)).sortBy(_._1).map(_._2.toInt))
    /*- Toarray.zipWithIndex convertit un vecteur en : (0.69,0), (0.23,1), (0.65,2), (1.51,3) donc chaque élement a un indice de colonne ) */
    /* -Ensuite pour chaque couple on cherche le neurone auquel il est affecté (après avoir fait un tri sur les lignes et regrouper les lignes
    appartenant au neurone 0 au début ( rowOrderData)) donc on lui donne l'indice, puis il cherche a quel colonne il a été affecté ) .
    Ensuite, on tri les affectations des colonnes .*/
    /*- Ainsi, cette ligne de code retourne un RDD qui contient les valeurs des éléments de rowOrderData triées par affectation de neurone.*/

  }

    def findBestRowNeuron(rowData: Vector): Int = {
      // Find best row neuron
      var bestRowNeuron = Int.MaxValue
      var minDist = Double.MaxValue
      for (rowNeuronId <- 0 until nbRowNeuron) {
        val tmpDist = rowSquaredDistance(rowNeuronId, rowData)
        if (tmpDist < minDist) {
          minDist = tmpDist
          bestRowNeuron = rowNeuronId
        }
      }
      bestRowNeuron
    }

    def rowSquaredDistance(rowNeuronId: Int, rowData: Vector): Double = {
      var ans = 0.0
      for (colId <- rowData.toArray.indices) {
        val colNeuronId = _colNeuronAffectation(colId)
        val valNeuron = _neuronMatrix(rowNeuronId, colNeuronId)
        val valData = rowData(colId)
        ans += (valNeuron - valData) * (valNeuron - valData)
      }
      ans
    }
    /*
   def quantError(): Double = datas.map{d => rowSquaredDistance(findBestRowNeuron(d.toVector()), d.toVector())}.reduce(_+_)

  def purity(dataset: RDD[NamedVector]): Double = {
    //val nbRealClass = dataset.map(_.cls).reduce(case(cls1,cls2))

    val sumAffectedDatas = dataset.map(d => ((this.findBestRowNeuron(d.toVector()), d.cls), 1))
      .reduceByKey{case (sum1, sum2) => sum1+sum2}

    val maxByCluster = sumAffectedDatas.map(sa => (sa._1._1, sa._2))
      .reduceByKey{case (sum1, sum2) => sum1.max(sum2) }
      .map(_._2)
      .collect()

    maxByCluster.sum / dataset.count().toDouble
  }

  def affectations(dataset: RDD[Array[Double]]): RDD[(Int, Int)] = {
    dataset.map(d => (d.cls, this.findBestRowNeuron(d.toVector())))
  }
*/


}

