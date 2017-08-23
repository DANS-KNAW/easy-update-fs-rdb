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

import org.rogach.scallop.{ ScallopConf, ScallopOption }

class CommandLineOptions(args: Seq[String], configuration: Configuration) extends ScallopConf(args) {

  appendDefaultToDescription = true
  editBuilder(_.setHelpWidth(110))

  printedName = "easy-update-fs-rdb"
  version(s"$printedName ${ configuration.version }")
  banner(s"""
            | Update the EASY File-system RDB with data about files and folders from the EASY Fedora Commons Repository.
            |
            | Usage: $printedName [--file|-f <text-file-with-dataset-id-per-line> | --dataset-pids|-d <dataset-pid>...]
            |
            | Options:
            |""".stripMargin)

  val datasetPidsFile: ScallopOption[File] = opt[File]("file",
    descr = "Text file with a dataset-id per line",
    short = 'f')
  val datasetPids: ScallopOption[List[String]] = opt[List[String]]("dataset-pids",
    descr = "ids of datasets for which to update the file and folder metadata in the File-system RDB",
    short = 'd', required = false)

  requireOne(datasetPidsFile, datasetPids)

  verify()
}
