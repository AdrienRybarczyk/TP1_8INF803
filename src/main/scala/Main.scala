import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.jsoup.Jsoup

import scala.collection.mutable.ArrayBuffer

object Main extends App {

  var tabSpell = ArrayBuffer[Sort]()

  for(i <- 1 to 1600) {
    println("Iteration " + i)
    val doc = Jsoup.connect("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=" + i).get()

    ///NAME
    val name = doc.select(".heading p").text()
    //println(name)

    val desc = doc.select(".SPDet")

    //Level //////
    val data_level = desc.get(0).text()
    val listLevel = data_level.split("; Level ")(1)
    var valueLevel = 5
    if(listLevel.indexOf("sorcerer/wizard")!= -1){
      val level = listLevel.split("sorcerer/wizard ")(1)
      valueLevel = level.split(",")(0).toInt
    }

    //Component /////
    val component = desc.get(2).text()
    //println(component)

    val tabComponent = component.split("Components ")
    //println(tabComponent(1))
    val tabComponentTrunc = tabComponent(1).split(",")
    val componentList = new ArrayBuffer[String]
    for (i <- 0 until tabComponentTrunc.length) {
      if(i> 0 && tabComponentTrunc(i).indexOf(" ")!= -1){
        tabComponentTrunc(i) = tabComponentTrunc(i).split(" ")(1)
      }
      componentList += tabComponentTrunc(i)
    }
    //println(componentList)

    //Spell Resistance ////
    val data_speelresistance = desc.get(desc.size()-1).text()
    //println(data_speelresistance)
    var spellResistance = true
    if(data_speelresistance.indexOf("Spell Resistance")!= -1){
      val spellresistance = data_speelresistance.split("Spell Resistance")(1)
      val valuespellresistance = spellresistance.split(" ")(1)
      if(valuespellresistance=="yes"){
        spellResistance = true
      }else{
        spellResistance = false
      }
    }else{
      spellResistance = false
    }
    tabSpell += new Sort(name,valueLevel,componentList,spellResistance)
  }

  /*val conf = new SparkConf()
    .setAppName("Save Pito")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")*/

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Save Pito").getOrCreate()

  val sqlContext = spark.sqlContext
  val rddSpells: RDD[Sort] = spark.sparkContext.makeRDD(tabSpell)
  val resultat = rddSpells.collect().filter(e => {
    e.level <= 4 && e.componentList.contains("V") && e.componentList.length == 1
  })
  for (i <- resultat.indices) {
    println(resultat(i).name + " : " + resultat(i).level + " : "+ resultat(i).componentList + " : "+ resultat(i).spellResistance)
  }
println("Total resultat "+ resultat.length)

  val result: RDD[Row] = rddSpells.map(f=> Row(f.name,f.level,f.componentList,f.spellResistance))
  val fields = Array(
    StructField("Name", StringType, nullable = false),
    StructField("Level", IntegerType, nullable = false),
    StructField("ComponentList", ArrayType(StringType)),
    StructField("Spell Resistance", BooleanType, nullable = false)
  )
  val df = spark.createDataFrame(result,StructType(fields))

  val spellsToSavePito: Dataset[Row] = df.select("Name")
    .where(array_contains(df("ComponentList"), "V"))
    .where(size(df("ComponentList")) === 1)
    .where(df("level") <= 4)
  println(spellsToSavePito.collect().foreach(println))
  println("Total resultat version Spark "+ spellsToSavePito.count())

}

class Sort (var name: String,
           var level: Int,
           var componentList: ArrayBuffer[String],
           var spellResistance: Boolean
)extends Serializable