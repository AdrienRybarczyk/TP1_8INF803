import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRank extends App {
  val conf = new SparkConf()
    .setAppName("PageRank")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val pageA = ("PageA",List("PageB","PageC"))
  val pageB = ("PageB",List("PageC"))
  val pageC = ("PageC",List("PageA"))
  val pageD = ("PageD",List("PageD"))
  val pagerank = 1.0
  var graph = List(
    pageA, pageB, pageC, pageD
  )
 val links = sc.parallelize(graph).partitionBy(new HashPartitioner(4)).persist()
 var ranks = links.mapValues(v => pagerank)

  for (i <- 1 to 20){
    val contributions = links.join(ranks).flatMap { case (url, (links, rank)) => links.map(dest => (dest, rank / links.size)) }
    contributions.collect
    ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
    ranks.collect

  }
  ranks.foreach(println)

}
