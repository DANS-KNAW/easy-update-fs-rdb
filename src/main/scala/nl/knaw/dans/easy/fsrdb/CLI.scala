package nl.knaw.dans.easy.fsrdb

import java.io.File

import com.yourmediashelf.fedora.client.FedoraCredentials
import org.apache.commons.configuration.PropertiesConfiguration

object CLI {
  def main(args: Array[String]): Unit = {
    val props = new PropertiesConfiguration(new File(System.getProperty("app.home"), "cfg/application.properties"))
    val conf = new Conf(args, props)
    implicit val settings = Settings(
      fedoraCredentials = new FedoraCredentials(conf.fedora(), conf.user(), conf.password()),
      postgresURL = conf.db(), conf.dataset())
    FsRdbUpdater.run.get
  }
}
