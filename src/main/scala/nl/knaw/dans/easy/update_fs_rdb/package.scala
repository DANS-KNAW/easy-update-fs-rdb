package nl.knaw.dans.easy

import com.yourmediashelf.fedora.client.FedoraCredentials

import scala.util.{Success, Failure, Try}

package object update_fs_rdb {

  case class Settings(fedoraCredentials: FedoraCredentials,
                      datasetPid: String)

  abstract class DigitalObject(pid: String,
                               parentSid: String,
                               datasetSid: String,
                               path: String)

  case class EasyFolder(pid: String,
                        parentSid: String,
                        datasetSid: String,
                        path: String,
                        name: String) extends DigitalObject(pid, parentSid, datasetSid, path)

  case class EasyFile(pid: String,
                      parentSid: String,
                      datasetSid: String,
                      path: String,
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
