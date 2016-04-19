package scalapgq.scalalike

import scalapgq._
import scalikejdbc._

class ScalikePGQOperationsSpec extends PGQOperationsSpec {
  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(enabled = true, singleLineMode = true)
  
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
  val consumerOps = new PGQConsumerOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}

class ScalikePGQConsumerSpec extends PGQConsumerSpec {
  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(enabled = true, singleLineMode = true)
  
  val PGQ = new PGQ(s => new PGQConsumerOperationsImpl(s.url, s.user, s.password))
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}
