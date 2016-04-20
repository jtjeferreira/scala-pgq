package scalapgq.scalalike

import scalapgq._
import scalikejdbc._

class ScalikePGQOperationsSpec extends AbstractPGQOperationsSpec {
  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(enabled = true, singleLineMode = true)
  
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}

class ScalikePGQConsumerSpec extends AbstractPGQConsumerSpec {
  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(enabled = true, singleLineMode = true)
  
  val PGQ = new PGQ(s => new PGQOperationsImpl(s.url, s.user, s.password))
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}
