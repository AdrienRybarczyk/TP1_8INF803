import org.jsoup.Jsoup

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Main extends App {

  //var list =ArrayBuffer[]

  for(i <- 1 to 1600) {
    println("Iteration " + i)
    val doc = Jsoup.connect("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=" + i).get()


    ///NAME
    val name = doc.select(".heading p").text()
    //println(name)
    val listName = List(("name", name))


    val desc = doc.select(".SPDet")
    //println(desc)

    //Level //////

    val data_level = desc.get(0).text()
    //println(data_level)
    val listLevel = data_level.split("; Level ")(1)
    //println(listLevel)
    val listLevelTab = listLevel.split(", ")
    val tabListLevel = new ArrayBuffer[String]
    for (i <- 0 until listLevelTab.length) {
      tabListLevel += listLevelTab(i)
    }
    val levelList = List(("Level", tabListLevel))
    //println(levelList)


    //Component /////

    val component = desc.get(2).text()
    //println(component)

    val tabComponent = component.split("Components ")
    val tabComponentTrunc = tabComponent(1).split(",")
    val tabComponentArrayBuffer = new ArrayBuffer[String]
    for (i <- 0 until (tabComponentTrunc.length - 1)) {
      if(i> 0 & tabComponentTrunc(i).indexOf(" ")!= -1){
        tabComponentTrunc(i) = tabComponentTrunc(i).split(" ")(1)
        //println(tabComponentTrunc(i))
      }
      tabComponentArrayBuffer += tabComponentTrunc(i)
    }
    val componentList = List(("Components", tabComponentArrayBuffer))
    //println(componentList)


    //Spell Resistance ////
    val data_speelresistance = desc.get(desc.size()-1).text()
    //println(data_speelresistance)
    var listSpeelResistance = new ListBuffer[String]()
    if(data_speelresistance.indexOf("Speel Resistance")!= -1){
      val speelresistance = data_speelresistance.split("; ")(1)
      //println(listSpeelResistance)
      listSpeelResistance +=("Speel Resistance",speelresistance.split(" ")(2))
      //println(tabSpeelResistance)
    }else{
      listSpeelResistance += ("Speel Resistance","No")
    }
    //val arrayBufferElement = new List(listName,levelList,componentList,listSpeelResistance)
    //list = list + new (name,)
  }

}
