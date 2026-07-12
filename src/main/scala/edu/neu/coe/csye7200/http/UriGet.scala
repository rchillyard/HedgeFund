package edu.neu.coe.csye7200.http

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.{Authority, Host, Path}

/**
  * @author robinhillyard
  */
class UriGet {
  def get(host: String, path: String, queryParams: Map[String, String], scheme: String = "https"): Uri =
    Uri(scheme, Authority(Host(host)), Path(path)).withQuery(Uri.Query(queryParams))
}