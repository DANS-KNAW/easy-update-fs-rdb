/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.fsrdb

import java.sql.{Connection, DriverManager}

import com.yourmediashelf.fedora.client.FedoraClient
import com.yourmediashelf.fedora.client.request.FedoraRequest
import nl.knaw.dans.lib.error._
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}
import scalaj.http.Http

object FsRdbUpdater {
  val loadDriver = classOf[org.postgresql.Driver]
  val log = LoggerFactory.getLogger(getClass)

  val NS_EASY_FILE = "easy-file"
  val NS_EASY_FOLDER = "easy-folder"
  val namespaces = List(NS_EASY_FILE, NS_EASY_FOLDER)

  def run(implicit s: Settings): Try[Unit] = {
    log.info(s"$s")

    val result = if (s.datasetPidsFile.isDefined)
      updateDatasets(Source.fromFile(s.datasetPidsFile.get).getLines.toList)
    else if (s.datasetPids.isDefined)
      updateDatasets(s.datasetPids.get)
    else
      Failure(new IllegalArgumentException("No datasets specified to update"))

    result match {
      case Failure(ex) => log.info(s"Failures : ${ex.getMessage}")
      case Success(_) => log.info("All completed succesfully")
    }
    result
  }

  private def updateDatasets(datasetPids: List[String])(implicit s: Settings): Try[Unit] = {
    for {
      _ <- Try {FedoraRequest.setDefaultClient(new FedoraClient(s.fedoraCredentials))}
      _ = log.info(s"Set fedora client")
      conn <- Try {DriverManager.getConnection(s.postgresURL)}
      _ = log.info(s"Connected to postgres")
      _ <- Try(conn.setAutoCommit(false))
      _ = log.info(s"Start updating ${datasetPids.size} dataset(s)")
      _ <- datasetPids.map(datasetPid => updateDataset(conn, datasetPid)).collectResults
      _ = conn.close()
    } yield log.info("Completed succesfully")
  }

  private def updateDataset(conn: Connection, datasetPid: String)(implicit s: Settings): Try[Unit] = {
    log.info(s"Checking if dataset $datasetPid exists")
    val result = for {
      _ <- existsDataset(datasetPid)
      _ = log.info(s"Dataset $datasetPid exists; Getting digital objects")
      pids <- findPids(datasetPid)
      _ = pids.foreach(pid => log.debug(s"Found digital object: $pid"))
      items <- getItems(datasetPid)(pids).collectResults.map(_.sortBy(_.path))
      _ = log.info("Updating database")
      _ <- deleteDatasetItems(conn, datasetPid)
      _ <- addDatasetItems(conn, items)
      _ <- Try (conn.commit())
    } yield log.info(s"Dataset $datasetPid updated succesfully")
    result.recoverWith { // TODO Replace with doOnError when we add it to dans-scala-lib
      case t: Throwable =>
        conn.rollback()
        Failure(t)
    }
  }

  private def existsDataset(datasetPid: String)(implicit s: Settings): Try[Unit] = Try {
    if(FedoraClient.findObjects()
      .pid().query(s"pid~$datasetPid").execute().getPids.isEmpty)
      throw new RuntimeException(s"Dataset not found: $datasetPid")
  }

  private def getItems(datasetPid: String)(pids: List[String])(implicit s: Settings): List[Try[Item]] = {
    pids.map(pid =>
      if (pid.startsWith(NS_EASY_FILE))
        getObjectXML(pid).flatMap(getFileItem(datasetPid)(pid))
      else if (pid.startsWith(NS_EASY_FOLDER))
        getObjectXML(pid).flatMap(getFolderItem(datasetPid)(pid))
      else
        Failure(new RuntimeException(s"Unknown namespace for PID: $pid")))
  }

  private def getObjectXML(pid: String): Try[Elem] = Try {
    XML.load(FedoraClient.getObjectXML(pid).execute().getEntityInputStream)
  }

  private def getFileItem(datasetPid: String)(filePid: String)(objectXML: Elem)(implicit s: Settings): Try[FileItem] = Try {
    val result = for {
      metadataDS <- objectXML \ "datastream"
      if (metadataDS \ "@ID").text == "EASY_FILE_METADATA"
      metadata <- metadataDS \ "datastreamVersion" \ "xmlContent" \ "file-item-md"
      relsExtDS <- objectXML \ "datastream"
      if (relsExtDS \ "@ID").text == "RELS-EXT"
      isMemberOf <- relsExtDS \ "datastreamVersion" \ "xmlContent" \ "RDF" \ "Description" \ "isMemberOf"
      parentSid = isMemberOf.attribute("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "resource").get
      fileDS <- objectXML \ "datastream"
      if (fileDS \ "@ID").text == "EASY_FILE"
      digest = fileDS \ "datastreamVersion" \ "contentDigest" \ "@DIGEST"
    } yield FileItem(
        pid = filePid,
        parentSid = parentSid.text.replace("info:fedora/", ""),
        datasetSid = datasetPid,
        path = (metadata \ "path").text,
        filename = (metadata \ "name").text,
        size = (metadata \ "size").text.toLong,
        mimetype = (metadata \ "mimeType").text,
        creatorRole = (metadata \ "creatorRole").text,
        visibleTo = (metadata \ "visibleTo").text,
        accessibleTo = (metadata \ "accessibleTo").text,
        sha1Checksum = if(digest.nonEmpty) digest.text else null)
    if (result.size != 1)
      throw new RuntimeException(s"Inconsistent file digital object, please inspect $filePid manually.")
    result.head
  }

  private def getFolderItem(datasetPid: String)(folderPid: String)(objectXML: Elem)(implicit s: Settings): Try[FolderItem] = Try {
    val result = for {
      metadataDS <- objectXML \ "datastream"
      if (metadataDS \ "@ID").text == "EASY_ITEM_CONTAINER_MD"
      metadata <- metadataDS \ "datastreamVersion" \ "xmlContent" \ "item-container-md"

      relsExtDS <- objectXML \ "datastream"
      if (relsExtDS \ "@ID").text == "RELS-EXT"
      isMemberOf <- relsExtDS \ "datastreamVersion" \ "xmlContent" \ "RDF" \ "Description" \ "isMemberOf"
      parentSid = isMemberOf.attribute("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "resource").get
    } yield FolderItem(
        pid = folderPid,
        parentSid = parentSid.text.replace("info:fedora/", ""),
        datasetSid = datasetPid,
        path = (metadata \ "path").text,
        name = (metadata \ "name").text)
    if (result.size != 1)
      throw new RuntimeException(s"Inconsistent folder digital object, please inspect $folderPid manually.")
    result.head
  }

  private def findPids(datasetPid: String)(implicit s: Settings): Try[List[String]] = Try {
    val url = s"${s.fedoraCredentials.getBaseUrl}/risearch"
    val response = Http(url)
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
      .param("type", "tuples")
      .param("lang", "sparql")
      .param("format", "CSV")
      .param("query",
        s"""
           |select ?s
           |from <#ri>
           |where { ?s <http://dans.knaw.nl/ontologies/relations#isSubordinateTo> <info:fedora/$datasetPid> . }
        """.stripMargin)
      .asString
    if (response.code != 200)
      throw new RuntimeException(s"Failed to query fedora resource index ($url), response code: ${response.code}")
    response.body.lines.toList.drop(1)
      .map(_.replace("info:fedora/", ""))
      .filter(pid => namespaces.exists(pid.startsWith))
  }

  private def addDatasetItems(conn: Connection, items: List[Item])(implicit s: Settings): Try[Unit] = {
    items.map {
        case folder: FolderItem => insertFolder(conn, folder)
        case file: FileItem => insertFile(conn, file)
    }.collectResults.map(_ => ())
  }

  private def insertFolder(conn: Connection, folder: FolderItem): Try[Unit] = Try {
    log.debug(s"Attempting to insert ${folder.pid}")
    val statement = conn.prepareStatement("INSERT INTO easy_folders (pid,path,name,parent_sid,dataset_sid) VALUES (?,?,?,?,?)")
    statement.setString(1, folder.pid)
    statement.setString(2, folder.path)
    statement.setString(3, folder.name)
    statement.setString(4, folder.parentSid)
    statement.setString(5, folder.datasetSid)
    statement.executeUpdate()
    statement.closeOnCompletion()
  }

  private def insertFile(conn: Connection, file: FileItem): Try[Unit] = Try {
    log.debug(s"Attempting to insert ${file.pid}")
    val statement = conn.prepareStatement("""
      INSERT INTO easy_files
        (pid, parent_sid, dataset_sid, path, filename, size, mimetype,
         creator_role, visible_to, accessible_to, sha1checksum)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)
      """   )
    statement.setString(1, file.pid)
    statement.setString(2, file.parentSid)
    statement.setString(3, file.datasetSid)
    statement.setString(4, file.path)
    statement.setString(5, file.filename)
    statement.setLong(6, file.size)
    statement.setString(7, file.mimetype)
    statement.setString(8, file.creatorRole)
    statement.setString(9, file.visibleTo)
    statement.setString(10, file.accessibleTo)
    statement.setString(11, file.sha1Checksum)
    statement.executeUpdate()
    statement.closeOnCompletion()
  }

  private def deleteDatasetItems(conn: Connection, datasetSid: String): Try[Unit] = Try {
    log.debug(s"Attempting to delete files and folders for dataset $datasetSid")
    val deleteFilesStatement = conn.prepareStatement("DELETE FROM easy_files WHERE dataset_sid = ?")
    deleteFilesStatement.setString(1, datasetSid)
    val deleteFoldersStatement = conn.prepareStatement("DELETE FROM easy_folders WHERE dataset_sid = ?")
    deleteFoldersStatement.setString(1, datasetSid)
    deleteFilesStatement.executeUpdate()
    deleteFilesStatement.closeOnCompletion()
    deleteFoldersStatement.executeUpdate()
    deleteFoldersStatement.closeOnCompletion()
  }

}
