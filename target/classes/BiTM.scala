import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.{Vector, Vectors}
import java.util.Random
import scala.math.abs
import utils.{Matrix}
import scala.collection.mutable


class BiTM(val nbRowNeuron: Int, val nbColNeuron: Int, datas: RDD[Array[Double]], val topoFactor: TopoFactor) extends Serializable {
  def this(nbRowNeuron: Int, nbColNeuron: Int, datas: RDD[Array[Double]]) = this(nbRowNeuron, nbColNeuron, datas, BiTMTopoFactor)


  private val _nbDataCol = datas.first().length
  private val _nbNeurons = nbRowNeuron * nbColNeuron

  /**
     Initialisation aléatoire des prototypes.
  */

  private val _rand = new Random()
  private var _colNeuronAffectation = Array.tabulate(_nbDataCol) { i => _rand.nextInt(_nbNeurons) }
  private var _neuronMatrix = initNeurons(_nbNeurons)



  protected def initNeurons(nbNeurons: Int): Matrix = {

    val rand = new Random()
    val samples = datas.takeSample(withReplacement = false, nbNeurons, rand.nextInt())

    new Matrix(samples.map { samp =>
      Array.tabulate(nbNeurons) { i =>
        samp(rand.nextInt(samp.length))
      }
    })
  }


      /**
          La méthode training() de la classe BiTM implémente l'algorithme d'apprentissage du modèle de 
          clustering BiTM en effectuant plusieurs itérations pour ajuster les affectations des neurones aux données.
      */
    def training(nbIter: Int) {

      for (iter <- 0 until nbIter) {



        // Affectation des colonnes
        _colNeuronAffectation = new BiTMCol(_neuronMatrix, _nbNeurons, _nbDataCol, _colNeuronAffectation).computeColAffectation()




        // Affectation des lignes et re-calcul du modèle
        _neuronMatrix = new BiTMRow(_neuronMatrix, _nbNeurons, _colNeuronAffectation, nbIter, iter).computeNewNeurons


      }
    }


    /**
      La classe BiTMCol est utilisée pour affecter chaque colonne de la matrice de données à un neurone dans la matrice de neurones.
    */
    class BiTMCol(neuronMatrix: Matrix, nbNeurons: Int, nbDataCol: Int, colNeuronAffectation: Array[Int]) extends Serializable {
      
      /**
        computeColAffectation() calcule les distances entre chaque colonne de la matrice de données et chaque neurone de la matrice de neurones;
      */
      
      def computeColAffectation(): Array[Int] = {

        val distByColNeuron = datas.map(d => Vectors.dense(d)).flatMap(localColDists).reduceByKey(_ + _)
        val colsBestNeuron = distByColNeuron.map(d => mapToNeuronDist(d._1, d._2)).reduceByKey(minNeuronDist).
          map(d => (d._1, d._2.colNeuron)).collectAsMap()

        Array.tabulate(nbDataCol)(i => colsBestNeuron(i))
      }

      
      protected def localColDists(dataRow: Vector) = Array.tabulate(nbDataCol * nbNeurons) { i =>
        val neuronColId = i % nbNeurons
        val neuronRowId = findBestRowNeuron(dataRow)
        val dataColId = i / nbNeurons
        val dataVal = dataRow(dataColId)
        val neuronVal = neuronMatrix(neuronRowId, neuronColId)
        val localDist = (neuronVal - dataVal) * (neuronVal - dataVal)
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


    /**
       BiTMRow a pour but de calculer les nouvelles valeurs des neurones.    
    */
    class BiTMRow(neuronMatrix: Matrix, nbNeurons: Int, colNeuronAffectation: Array[Int], maxIter: Int, currentIter: Int)
      extends Serializable {
      def computeNewNeurons: Matrix = {


        val allObs = datas.map(d => Vectors.dense(d)).map(d => mapperRowAffectation(d))



        // géneration des nouvelles valeurs avec les matrices CM et CN .
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
        // Recherche du meilleur neurone pour chaque vecteur
        val rowBestN = findBestRowNeuron(dataRow)

        // Calcul de la distance avec la formule
        val topoFactors = topoFactor.gen(maxIter, currentIter, nbNeurons)

        val obs = Array.fill(nbNeurons, nbNeurons)(new ObsElem())
        for (c <- 0 until dataRow.size) {
          val colBestN = colNeuronAffectation(c)
          val elem = dataRow(c)

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
       
        /**
         Find best row neuron
        */
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
    /**
    Pour chaque ligne du dataset on lui cherche le meilleur neurone ( findBestRowNeuron)  et on retourne le vecteur associé et le neurone associé.
    */

    val rowOrderData = affData.sortByKey().values
    /**
      On trie affData par les clés qui sont les indices des neuronnes auxquel ils sont affectés.
    */



    rowOrderData.map(_.toArray.zipWithIndex.map(d => (_colNeuronAffectation(d._2), d._1)).sortBy(_._1).map(_._2.toInt))
    /**
     Toarray.zipWithIndex convertit un vecteur en : (0.69,0), (0.23,1), (0.65,2), (1.51,3) donc chaque élement a un indice de colonne ). 
     
     Ensuite pour chaque couple on cherche le neurone auquel il est affecté (après avoir fait un tri sur les lignes et regrouper les lignes
    appartenant au neurone 0 au début ( rowOrderData)
    
     donc on lui donne l'indice, puis il cherche a quel colonne il a été affecté.
    Ensuite, on tri les affectations des colonnes .
    
     Ainsi, cette ligne de code retourne un RDD qui contient les valeurs des éléments de rowOrderData triées par affectation de neurone.
    */

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



}

