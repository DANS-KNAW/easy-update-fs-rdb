/*
 * Copyright (C) 2015 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.fsrdb.command

import java.nio.file.Paths

import com.yourmediashelf.fedora.client.FedoraCredentials
import nl.knaw.dans.lib.error._

import nl.knaw.dans.easy.fsrdb.{Settings, FsRdbUpdater}

object Command extends App {
  val configuration = Configuration(Paths.get(System.getProperty("app.home")))
  val clo = new CommandLineOptions(args, configuration)
  implicit val settings: Settings = Settings(
    fedoraCredentials = new FedoraCredentials(
      configuration.properties.getString("default.fcrepo-server"),
      configuration.properties.getString("default.fcrepo-user"),
      configuration.properties.getString("default.fcrepo-password")),
    databaseUrl = configuration.properties.getString("default.db-connection-url"),
    databaseUser = configuration.properties.getString("default.db-connection-username"),
    databasePassword = configuration.properties.getString("default.db-connection-password"),
    datasetPidsFile = clo.datasetPidsFile.toOption,
    datasetPids = clo.datasetPids.toOption)

  FsRdbUpdater.run
    .doIfSuccess(_ => println("OK: All completed successfully"))
    .doIfFailure { case e => println(s"FAILED: ${ e.getMessage }") }
}
