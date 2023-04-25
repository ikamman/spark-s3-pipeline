package com.spark.home.assignment

import org.rogach.scallop.ScallopConf

class ArgConfig(args: Seq[String]) extends ScallopConf(args) {

  val input = opt[String](required = true)
  val output = opt[String](required = true)
  val local = toggle(required = false, default = Some(false))
  val profile = opt[String](required = true, default = Some("default"))
  val credentials = opt[String]()
  verify()
}
