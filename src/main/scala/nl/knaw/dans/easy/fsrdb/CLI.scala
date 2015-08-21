package nl.knaw.dans.easy.fsrdb

import com.yourmediashelf.fedora.client.FedoraCredentials

object CLI {
  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    implicit val settings = Settings(
      fedoraCredentials = new FedoraCredentials(conf.fedora(), conf.user(), conf.password()),
      postgresURL = conf.db(), conf.dataset())
    FsRdbUpdater.run.get
  }
}
