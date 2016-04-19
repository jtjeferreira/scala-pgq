package scalapgq.scalalike

import scalapgq._

class ScalikePGQOperationsSpec extends PGQOperationsSpec {
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
  val consumerOps = new PGQConsumerOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}

class ScalikePGQConsumerSpec extends PGQConsumerSpec {
  val PGQ = new PGQ(s => new PGQConsumerOperationsImpl(s.url, s.user, s.password))
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}
