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

import java.io.File
import java.net.URL

import org.apache.commons.configuration.PropertiesConfiguration
import org.rogach.scallop.ScallopConf

class Conf(args: Seq[String], props: PropertiesConfiguration) extends ScallopConf(args) {
  printedName = "easy-update-fs-rdb"
  version(s"$printedName ${Version()}")
  banner(s"""
            | Update the EASY File-system RDB with data about files and folders from the EASY Fedora Commons Repository.
            |
            | Usage: $printedName
            |              [-f <fcrepo-server>] \\
            |              [-u <fcrepo-user> \\
            |               -p <fcrepo-password>] \\
            |              [-d <db-connection-url>] \\
            |               <dataset-pid>...
            | Options:
            |""".stripMargin)
  val fedora = opt[URL]("fcrepo-server", short = 'f',
    descr = "Fedora Commons Repository Server to connect to ",
    default = Some(new URL(props.getString("default.fcrepo-server"))))
  val user = opt[String]("fcrepo-user", short = 'u',
    descr = "User to connect to fcrepo-server",
    default = Some(props.getString("default.fcrepo-user")))
  val password = opt[String]("fcrepo-password", short = 'p',
    descr = "Password for fcrepo-user",
    default = Some(props.getString("default.fcrepo-password")))
  val db = opt[String]("db-connection-url", short = 'd',
    descr="JDBC connection URL to File-system RDB (including user and password parameters)",
    default = Some(props.getString("default.db-connection-url")))
  val datasets = trailArg[List[String]]("dataset-pids",
    descr = "ids of datasets for which to update the file and folder metadata in the File-system RDB",
    required = true)
}