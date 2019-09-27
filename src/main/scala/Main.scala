import org.apache.spark.{SparkConf, SparkContext}
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
    for (i <- 0 until (tabComponentTrunc.length)) {
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

 /*for (i <- 0 until tabSpell.length) {
    println(tabSpell(i).name + " : " + tabSpell(i).level + " : "+ tabSpell(i).componentList + " : "+ tabSpell(i).speelResistance)
  }*/
  val conf = new SparkConf()
    .setAppName("Save Pito")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val test = sc.makeRDD(tabSpell)
  val resultat = test.collect().filter(e => {
    e.level <= 4 && e.componentList.contains("V") && e.componentList.length == 1
  })
  for (i <- 0 until resultat.length) {
    println(resultat(i).name + " : " + resultat(i).level + " : "+ resultat(i).componentList + " : "+ resultat(i).speelResistance)
  }
println("Total resultat "+ resultat.length)
}

class Sort (var name: String,
           var level: Int,
           var componentList: ArrayBuffer[String],
           var speelResistance: Boolean
)extends Serializable