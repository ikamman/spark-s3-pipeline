package com.spark.home.assignment

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.AWSCredentialsProvider

object Credentials {

  val defaultProvider = new DefaultAWSCredentialsProviderChain()

  def load(path: Option[String], profile: Option[String]): AWSCredentials =
    path
      .map(p => new ProfileCredentialsProvider(p, profile.getOrElse("default")))
      .getOrElse(defaultProvider.asInstanceOf[AWSCredentialsProvider])
      .getCredentials()
}
