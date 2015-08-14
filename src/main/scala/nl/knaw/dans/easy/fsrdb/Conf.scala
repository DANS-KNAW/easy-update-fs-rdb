package nl.knaw.dans.easy.fsrdb

import java.io.File
import java.net.URL

import org.apache.commons.configuration.PropertiesConfiguration
import org.rogach.scallop.ScallopConf

class Conf(args: Seq[String]) extends ScallopConf(args) {
  val props = new PropertiesConfiguration(new File(homedir, "cfg/application.properties"))
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
            |               <dataset-pid>
            | Options:
            |""".stripMargin)
  val fedora = opt[URL]("fcrepo-server",
    descr = "Fedora Commons Repository Server to connect to ",
    default = Some(new URL(props.getString("default.fcrepo-server"))))
  val user = opt[String]("fcrepo-user",
    descr = "User to connect to fcrepo-server",
    default = Some(props.getString("default.fcrepo-user")))
  val password = opt[String]("fcrepo-password",
    descr = "Password for fcrepo-user",
    default = Some(props.getString("default.fcrepo-password")))
  val db = opt[String]("db-connection-url",
    descr="JDBC connection URL to File-system RDB (including user and password parameters)",
    default = Some(props.getString("default.db-connection-url")))
  val dataset = trailArg[String]("dataset-pid",
    descr = "Dataset for which to update the file and folder metadata in the File-system RDB",
    required = true)
}