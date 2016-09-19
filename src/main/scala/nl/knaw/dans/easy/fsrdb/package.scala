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
package nl.knaw.dans.easy

import java.io.File

import com.yourmediashelf.fedora.client.FedoraCredentials

package object fsrdb {
  object Settings {
    /** Backward compatible for EasyIngestFlow */
    def apply(fedoraCredentials: FedoraCredentials,
              postgresURL: String,
              datasetPid: String
             ):Settings = new Settings(
      fedoraCredentials,
      postgresURL,
      datasetPidsFile = None,
      datasetPids = Some(List(datasetPid))
    )
  }
  case class Settings(fedoraCredentials: FedoraCredentials,
                      postgresURL: String,
                      datasetPidsFile: Option[File] = None,
                      datasetPids: Option[List[String]] = None)

  abstract class Item(val pid: String,
                               val parentSid: String,
                               val datasetSid: String,
                               val path: String)

  case class FolderItem(override val pid: String,
                        override val parentSid: String,
                        override val datasetSid: String,
                        override val path: String,
                        name: String) extends Item(pid, parentSid, datasetSid, path)

  case class FileItem(override val pid: String,
                      override val parentSid: String,
                      override val datasetSid: String,
                      override val path: String,
                      filename: String,
                      size: Long,
                      mimetype: String,
                      creatorRole: String,
                      visibleTo: String,
                      accessibleTo: String,
                      sha1Checksum: String) extends Item(pid, parentSid, datasetSid, path)
}
