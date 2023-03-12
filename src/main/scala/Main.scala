
import breeze.linalg.DenseVector
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.{DataGenerator, NamedVector}

import java.security.Permission
import java.util.Random





//import org.altic.spark.clustering.utils.{NamedVector, DataGenerator}

//object RunBiTM extends App {
object Main {

  val conf = new SparkConf().setAppName("Mon application Spark").setMaster("local[*]")
  val sc = new SparkContext(conf)
  def main(args: Array[String]) {
    println("Hey");


    val nbRowSOM = 10
    val nbColSOM = 10
    val nbIter = 10
    val dataNbObs = 4
    val dataNbVars = 2


    class AffectedVector(var rowNeuronId: Int, elements: Array[Double]) extends DenseVector(elements) {
      def indices = elements.indices
    }

    def parseData(line: String): AffectedVector = {
      // TODO : Remplacer 0 par rand
      new AffectedVector(1, line.split(' ').map(_.toDouble))
    }





    // Read data from file
    //val lines = sc.textFile("/home/tug/ScalaProjects/spark/kmeans_data.txt")
    //val datas = lines.map(parseData _).cache()

    //val datas = sc.parallelize(Array.fill(1)(parseData("1")), 1)


   // val arrDatas = DataGenerator.gen2ClsNDims(dataNbObs, dataNbVars).getNamedVector
    //val arrDatas = Array(Vector(Array(1.0, 1.1)), Vector(Array(2.0, 2.1)))
    //val arrDatas = Array.tabulate(dataNbObs)(i => new Vector(Array.tabulate(dataNbVars)(j => i*20+j+10)))
    //val arrDatas = Array.tabulate(dataNbObs)(i => new AffectedVector(1, Array.tabulate(dataNbVars)(j => 1)))
    //val datas = sc.parallelize(arrDatas, 6)


    val myVector = Vectors.zeros(10000)


   //val datas :RDD[NamedVector] = sc.emptyRDD

    /*val datas = sc.
      textFile("C:\\Users\\33658\\IdeaProjects\\NPLBM\\src\\main\\waveform-5000_csv.csv").map
    (line => line.split(",").map(_.toDouble))*/

    val datas = sc.textFile("C:\\Users\\33658\\IdeaProjects\\NPLBM\\src\\main\\waveform-5000_csv.csv")
      .mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
      .map(line => line.split(",").map(_.toDouble).dropRight(1))

    val premiereLigne = datas.first()

    // Afficher la première valeur de la première ligne
    datas.collect().foreach(arr => {
      arr.foreach(element => {
        println(element)
      })
    })
    /*ON DEVRA PEUT ETRE AUSSI ENLEVER LA COLONNE CLASS ( COMME LE PROF AVAIT DIT )*/


    /*PEUT ETRE ON POURRA ABANDONNER LE NAMEDVECTOR ?*/
          //val datas = sc.textFile("/home/tug/pyImg/randImg.4colors.data").map(line => new Vector(line.split("\t").map(_.toDouble)))


          // Initialisation du model
        val model = new BiTM(nbRowSOM, nbColSOM,datas)
         //val model = new Croeuc(nbRowSOM * nbColSOM, datas)


          model.training(nbIter)

          val affData = model.affectation(datas)


    affData.first().foreach(element => {
        println(element)
      })

       sys.exit()
          // FAUT MODIF LE MAIN



  }
}