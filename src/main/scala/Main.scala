import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}

import scala.collection.mutable.ArrayBuffer

object Main extends App {

  //var list =ArrayBuffer[TOTO]

  val doc = Jsoup.connect("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=1").get()

  val name = doc.select(".heading p").text()
  println(name)

  val desc = doc.select(".SPDet")
  //println(desc)

  val data_level = desc.get(0).text()
  //println(data_level)
  val level = data_level.split("; ")(1)
  println(level)

  val component = desc.get(2).text()
  println(component)

  val data_speelresistance = desc.get(6).text()
  //println(data_speelresistance)
  val speelresistance = data_speelresistance.split("; ")(1)
  println(speelresistance)

  //list = list + new TOTO(name,)
}
