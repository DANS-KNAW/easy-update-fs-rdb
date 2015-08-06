package nl.knaw.dans.easy.update_fs_rdb

import com.yourmediashelf.fedora.client.request.FedoraRequest
import com.yourmediashelf.fedora.client.{FedoraClient, FedoraCredentials}
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.xml.{Elem, XML}
import scalaj.http.Http

object Main {
  val log = LoggerFactory.getLogger(getClass)

  val NS_EASY_FILE = "easy-file"
  val NS_EASY_FOLDER = "easy-folder"
  val namespaces = List(NS_EASY_FILE, NS_EASY_FOLDER)

  def main(args: Array[String]) {

    implicit val s = Settings(
      fedoraCredentials = new FedoraCredentials("http://deasy:8080/fedora", "fedoraAdmin", "fedoraAdmin"),
      pid = "easy-dataset:39")

    FedoraRequest.setDefaultClient(new FedoraClient(s.fedoraCredentials))

    val result = for {
      pids <- findPids()
      _ = pids.foreach(log.info)
      xs <- getDigitalObjects(pids)
    } yield println(xs.head)

    result.get
  }

  def getDigitalObjects(pids: List[String])(implicit s: Settings): Try[List[DigitalObject]] = Try {
    pids.map(pid =>
      if (pid.startsWith(NS_EASY_FILE)) {
        val objectXML = XML.load(FedoraClient.getObjectXML(pid).execute().getEntityInputStream)
        getEasyFile(pid, objectXML)
      }
      else if (pid.startsWith(NS_EASY_FOLDER)) {
        val xml = XML.load(FedoraClient.getDatastream(pid, "EASY_ITEM_CONTAINER_MD").execute().getEntityInputStream)
        getEasyFolder(pid, xml)
      }
      else {
        throw new RuntimeException(s"Unknown PID: $pid")
      }
    )
  }

  def getEasyFile(pid: String, objectXML: Elem)(implicit s: Settings): EasyFile = {
    val result = for {
      metadataDS <- objectXML \ "datastream"
      if (metadataDS \ "@ID").text == "EASY_FILE_METADATA"
      metadata <- metadataDS \ "datastreamVersion" \ "xmlContent" \ "file-item-md"

      fileDS <- objectXML \ "datastream"
      if (fileDS \ "@ID").text == "EASY_FILE"
      digest <- fileDS \ "datastreamVersion" \ "contentDigest" \ "@DIGEST"

      relsExtDS <- objectXML \ "datastream"
      if (relsExtDS \ "@ID").text == "RELS-EXT"
      isMemberOf <- relsExtDS \ "datastreamVersion" \ "xmlContent" \ "RDF" \ "Description" \ "isMemberOf"
      parentSid = isMemberOf.attribute("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "resource").get
    } yield EasyFile(
        pid = pid,
        parentSid = parentSid.text,
        datasetSid = s.pid,
        path = (metadata \ "path").text,
        filename = (metadata \ "name").text,
        size = (metadata \ "size").text.toInt,
        mimetype = (metadata \ "mimeType").text,
        creatorRole = (metadata \ "creatorRole").text,
        visibleTo = (metadata \ "visibleTo").text,
        accessibleTo = (metadata \ "accessibleTo").text,
        sha1checksum = digest.text)
    if (result.size != 1)
      throw new RuntimeException(s"Inconsistent digital object, please inspect $pid manually.")
    result.head
  }

  def getEasyFolder(pid: String, xml: Elem)(implicit s: Settings): EasyFolder = {
    EasyFolder(
      pid = pid,
      parentSid = "",
      datasetSid = s.pid,
      path = "",
      name = ""
    )
  }

  def findPids()(implicit s: Settings): Try[List[String]] = Try {
    val url = s"${s.fedoraCredentials.getBaseUrl}/risearch"
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
      .filter(pid => namespaces.exists(pid.startsWith))
  }
}
