package nl.knaw.dans.easy.update_fs_rdb

import org.slf4j.LoggerFactory

import scala.util.Try
import scalaj.http.Http

object Main {
  val log = LoggerFactory.getLogger(getClass)

  case class Settings(fedoraURL: String, pid: String, namespaces: List[String])

  abstract class DigitalObject(pid: String,
                               parentSid: String,
                               datasetSid: String,
                               path: String)

  case class EasyFolder(pid: String,
                        parentSid: String,
                        datasetSid: String,
                        path: String,
                        name: String) extends DigitalObject(pid, parentSid, datasetSid, path)

  case class EasyFile(pid: String,
                      parentSid: String,
                      datasetSid: String,
                      path: String,
                      filename: String,
                      size: Int,
                      mimetype: String,
                      creatorRole: String,
                      visibleTo: String,
                      accessibleTo: String,
                      sha1checksum: String) extends DigitalObject(pid, parentSid, datasetSid, path)

  def main(args: Array[String]) {

    implicit val s = Settings(
      fedoraURL = "http://deasy:8080/fedora",
      pid = "easy-dataset:39",
      namespaces = List("easy-folder", "easy-file"))

    val result = for {
      pids <- findPids()
    } yield pids.foreach(log.info)

    result.get
  }

  def findPids()(implicit s: Settings): Try[List[String]] = Try {
    val url = s"${s.fedoraURL}/risearch"
    val response = Http(url)
      .param("type", "tuples")
      .param("lang", "sparql")
      .param("format", "CSV")
      .param("query", s"select ?s from <#ri> where { ?s <http://dans.knaw.nl/ontologies/relations#isSubordinateTo> <info:fedora/${s.pid}> . }")
      .asString
    if (response.code != 200)
      throw new RuntimeException(s"Failed to query fedora resource index ($url), response code: ${response.code}")
    response.body.lines.toList.drop(1)
      .map(_.replace("info:fedora/", ""))
      .filter(pid => s.namespaces.exists(pid.startsWith))
  }
}
