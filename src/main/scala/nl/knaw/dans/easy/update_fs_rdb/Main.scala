package nl.knaw.dans.easy.update_fs_rdb

import org.slf4j.LoggerFactory

import scalaj.http.Http

object Main {
  val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {

    val fedoraURL = "http://deasy:8080/fedora"
    val pid = "easy-dataset:39"
    val namespaces = List("easy-folder", "easy-file")

    val response = Http(s"$fedoraURL/risearch")
      .param("type", "tuples")
      .param("lang", "sparql")
      .param("format", "CSV")
      .param("query", s"select ?s from <#ri> where { ?s <http://dans.knaw.nl/ontologies/relations#isSubordinateTo> <info:fedora/$pid> . }")
      .asString

    val pids = response.body.lines.drop(1).map(_.replace("info:fedora/", "")).filter(pid => namespaces.exists(pid.startsWith))

    pids.foreach(println)
  }
}
