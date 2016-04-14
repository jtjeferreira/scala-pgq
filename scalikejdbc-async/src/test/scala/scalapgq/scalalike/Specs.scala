package scalapgq.scalalike

import scalapgq._
import scalikejdbc.async._

class ScalikePGQOperationsSpec extends PGQOperationsSpec[TxAsyncDBSession] {
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}

class ScalikePGQConsumerSpec extends PGQConsumerSpec[TxAsyncDBSession] {
  val PGQ = new PGQ[TxAsyncDBSession](s => new PGQOperationsImpl(s.url, s.user, s.password))
  val ops = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
}
