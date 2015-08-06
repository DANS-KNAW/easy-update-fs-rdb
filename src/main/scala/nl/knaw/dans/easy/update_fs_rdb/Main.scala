package nl.knaw.dans.easy.update_fs_rdb

import com.yourmediashelf.fedora.client.request.FedoraRequest
import com.yourmediashelf.fedora.client.{FedoraClient, FedoraCredentials}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}
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
      datasetPid = "easy-dataset:39")

    FedoraRequest.setDefaultClient(new FedoraClient(s.fedoraCredentials))

    val result = for {
      pids <- findPids()
      _ = pids.foreach(log.info)
      xs <- getDigitalObjects(pids).sequence
    } yield xs.foreach(println)

    result.get
  }

  def getDigitalObjects(pids: List[String])(implicit s: Settings): List[Try[DigitalObject]] = {
    pids.map(pid =>
      if (pid.startsWith(NS_EASY_FILE)) {
        getObjectXML(pid).flatMap(getEasyFile(pid))
      }
      else if (pid.startsWith(NS_EASY_FOLDER)) {
        getObjectXML(pid).flatMap(getEasyFolder(pid))
      }
      else {
        Failure(new RuntimeException(s"Unknown namespace for PID: $pid"))
      }
    )
  }

  def getObjectXML(pid: String): Try[Elem] = Try {
    XML.load(FedoraClient.getObjectXML(pid).execute().getEntityInputStream)
  }

  def getEasyFile(pid: String)(objectXML: Elem)(implicit s: Settings): Try[EasyFile] = Try {
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
        parentSid = parentSid.text.replace("info:fedora/", ""),
        datasetSid = s.datasetPid,
        path = (metadata \ "path").text,
        filename = (metadata \ "name").text,
        size = (metadata \ "size").text.toInt,
        mimetype = (metadata \ "mimeType").text,
        creatorRole = (metadata \ "creatorRole").text,
        visibleTo = (metadata \ "visibleTo").text,
        accessibleTo = (metadata \ "accessibleTo").text,
        sha1checksum = digest.text)
    if (result.size != 1)
      throw new RuntimeException(s"Inconsistent file digital object, please inspect $pid manually.")
    result.head
  }

  def getEasyFolder(pid: String)(objectXML: Elem)(implicit s: Settings): Try[EasyFolder] = Try {
    val result = for {
      metadataDS <- objectXML \ "datastream"
      if (metadataDS \ "@ID").text == "EASY_ITEM_CONTAINER_MD"
      metadata <- metadataDS \ "datastreamVersion" \ "xmlContent" \ "item-container-md"

      relsExtDS <- objectXML \ "datastream"
      if (relsExtDS \ "@ID").text == "RELS-EXT"
      isMemberOf <- relsExtDS \ "datastreamVersion" \ "xmlContent" \ "RDF" \ "Description" \ "isMemberOf"
      parentSid = isMemberOf.attribute("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "resource").get
    } yield EasyFolder(
        pid = pid,
        parentSid = parentSid.text.replace("info:fedora/", ""),
        datasetSid = s.datasetPid,
        path = (metadata \ "path").text,
        name = (metadata \ "name").text)
    if (result.size != 1)
      throw new RuntimeException(s"Inconsistent folder digital object, please inspect $pid manually.")
    result.head
  }

  def findPids()(implicit s: Settings): Try[List[String]] = Try {
    val url = s"${s.fedoraCredentials.getBaseUrl}/risearch"
    val response = Http(url)
      .param("type", "tuples")
      .param("lang", "sparql")
      .param("format", "CSV")
      .param("query", s"select ?s from <#ri> where { ?s <http://dans.knaw.nl/ontologies/relations#isSubordinateTo> <info:fedora/${s.datasetPid}> . }")
      .asString
    if (response.code != 200)
      throw new RuntimeException(s"Failed to query fedora resource index ($url), response code: ${response.code}")
    response.body.lines.toList.drop(1)
      .map(_.replace("info:fedora/", ""))
      .filter(pid => namespaces.exists(pid.startsWith))
  }
}
