package scalapgq

import com.typesafe.config.ConfigFactory

trait PGQSpec {
  val config = ConfigFactory.load()
  
  val PostgresUrl = config.getString("db.url")
  val PostgresUser = config.getString("db.user")
  val PostgresPassword = config.getString("db.password")
}