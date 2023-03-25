import org.apache.spark.SparkContext
import org.apache.spark.{SparkConf}
import java.io.PrintWriter


object Main {

  val conf = new SparkConf().setAppName("Mon application Spark").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {


    /**
      Initialisation des valeurs d'entrée de BiTM.
    */
    val nbRowSOM = 10
    val nbColSOM = 10
    val nbIter = 10
    val dataNbObs = 4
    val dataNbVars = 2

    /** 
      Fichier Input ".csv" represntant le dataset a entrainé .
    */
    val datas = sc.textFile("C:\\Users\\lydid\\OneDrive\\Bureau\\ter\\Gitter\\ter-coclustering\\src\\main\\waveform-5000_csv.csv")
      .mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
      .map(line => line.split(",").map(_.toDouble).dropRight(1))

    /** 
      Initialisation du model ( Choisir BiTM ou Croeuc )
    */
    val model = new BiTM(nbRowSOM, nbColSOM, datas)
    //val model = new Croeuc(nbRowSOM * nbColSOM, datas)


    model.training(nbIter)
    val affData = model.affectation(datas)
    val csvData = affData.map(_.mkString(",")).collect()

    /**
     Fichier Output contenant le csv résultant.
    */
    val writer = new PrintWriter("C:\\Users\\lydid\\OneDrive\\Bureau\\ter\\Gitter\\test.csv")
    csvData.foreach(writer.println)
    writer.close()

    sys.exit()


  }
}