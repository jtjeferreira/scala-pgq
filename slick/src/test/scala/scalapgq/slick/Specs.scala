package scalapgq.slick

import scalapgq._
import _root_.slick.driver.PostgresDriver.api.Session

class SlickPGQOperationsSpec extends AbstractPGQOperationsSpec {
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}

class SlickPGQConsumerSpec extends AbstractPGQConsumerSpec {
  val PGQ = new PGQ(s => new PGQOperationsImpl(s.url, s.user, s.password))
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}
