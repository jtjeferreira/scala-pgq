package scalapgq.scalalike.async

import scalapgq._

class ScalikePGQOperationsSpec extends AbstractPGQOperationsSpec {
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}

class ScalikePGQConsumerSpec extends AbstractPGQConsumerSpec {
  val PGQ = new PGQ(s => new PGQOperationsImpl(s.url, s.user, s.password))
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}
