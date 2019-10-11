import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRank extends App {
  val conf = new SparkConf()
    .setAppName("PageRank MapReduce")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  //On crée notre graphe, on aura Nom de la page et la liste des noeuds adjacents
  val pageA = ("PageA",List("PageB","PageC"))
  val pageB = ("PageB",List("PageC"))
  val pageC = ("PageC",List("PageA"))
  val pageD = ("PageD",List("PageC"))
  val pagerank = 1.0
  var graph = List(
    pageA, pageB, pageC, pageD
  )
 val links: RDD[(String, List[String])] = sc.parallelize(graph).partitionBy(new HashPartitioner(4)).persist()
 var ranks: RDD[(String, Double)] = links.mapValues(v => pagerank)

  println("START")
  for (i <- 1 to 20){
    //On recupere les contributions de chaque noeud entrant et on les ajoute pour obtenir
    // la somme des contributions des autres noeuds au noeud actuel
    val contributions: RDD[(String, Double)] = links.join(ranks).flatMap { case (url, (links, rank)) =>
      links.map(dest => (dest, rank / links.size))
    }.reduceByKey((x,y)=>(x+y))
    contributions.collect

    //On récupère les noeuds n'ayant pas de lien entrant (dans notre cas la page D)
    val noeuds_sans_liens_entrants = links.subtractByKey(contributions)

    //On ajoute les autres pages et on assigne un pagerank de 0 aux pages qui n'ont pas de contribution
    val allPages = noeuds_sans_liens_entrants.map(page => (page._1,0.0)).union(contributions)

    //On modifie et sauvegarde la valeur du nouveau pagerank
    ranks = allPages.mapValues(v => 0.15 + 0.85*v)

    println("**************************************")
    println("Itération numéro : " + i)
    //Affichage :  Nom du noeud, pagerank
    ranks.sortByKey(true).collect.foreach(println)
  }
  println("END")
}
