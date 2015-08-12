package nl.knaw.dans.easy

import com.yourmediashelf.fedora.client.FedoraCredentials

import scala.util.{Success, Failure, Try}

package object update_fs_rdb {

  case class Settings(fedoraCredentials: FedoraCredentials,
                      postgresURL: String,
                      datasetPid: String)

  abstract class DigitalObject(val pid: String,
                               val parentSid: String,
                               val datasetSid: String,
                               val path: String)

  case class EasyFolder(override val pid: String,
                        override val parentSid: String,
                        override val datasetSid: String,
                        override val path: String,
                        name: String) extends DigitalObject(pid, parentSid, datasetSid, path)

  case class EasyFile(override val pid: String,
                      override val parentSid: String,
                      override val datasetSid: String,
                      override val path: String,
                      filename: String,
                      size: Int,
                      mimetype: String,
                      creatorRole: String,
                      visibleTo: String,
                      accessibleTo: String,
                      sha1checksum: String) extends DigitalObject(pid, parentSid, datasetSid, path)

  class CompositeException(throwables: List[Throwable]) extends RuntimeException(throwables.foldLeft("")((msg, t) => s"$msg\n${t.getMessage}"))

  implicit class ListTryExtensions[T](xs: List[Try[T]]) {
    def sequence: Try[List[T]] =
      if (xs.exists(_.isFailure))
        Failure(new CompositeException(xs.collect { case Failure(e) => e }))
      else
        Success(xs.map(_.get))
  }
}