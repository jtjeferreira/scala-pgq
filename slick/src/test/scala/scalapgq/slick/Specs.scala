package scalapgq.slick

import scalapgq._
import _root_.slick.driver.PostgresDriver.api.Session

class SlickPGQOperationsSpec extends PGQOperationsSpec {
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
  val consumerOps = new PGQConsumerOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}

class SlickPGQConsumerSpec extends PGQConsumerSpec {
  val PGQ = new PGQ(s => new PGQConsumerOperationsImpl(s.url, s.user, s.password))
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}
