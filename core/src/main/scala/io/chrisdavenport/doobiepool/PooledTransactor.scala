package io.chrisdavenport.doobiepool

// import cats.implicits._
import cats.effect._
// import cats.effect.concurrent._
import doobie.KleisliInterpreter
import doobie.util.transactor.Transactor
import doobie.util.transactor.Strategy
import scala.concurrent.ExecutionContext
import io.chrisdavenport.keypool._

import java.sql.{Connection, DriverManager}

object PooledTransactor {
  def apply[F[_]: Concurrent: Timer : ContextShift](
    driver: String,
    url:    String,
    user:   String,
    pass: String,
    maxConnections: Int,
    connectEC:  ExecutionContext,
    transactEC: ExecutionContext,
  ): Resource[F, Transactor[F]] = for {
    // sem <- Resource.liftF(Semaphore[F](maxConnections.toLong))
    pool <- KeyPool.create[F, Unit, Connection](
      {_: Unit => //sem.acquire >> 
        ContextShift[F].evalOn(connectEC)(
          Sync[F].delay { Class.forName(driver); DriverManager.getConnection(url, user, pass) }
        )},
      { case (_, conn) => //sem.release >> 
        ContextShift[F].evalOn(connectEC)(Sync[F].delay { conn.close() })},
      Reuse,
      Long.MaxValue,
      maxConnections,
      maxConnections,
      {_: Throwable => Sync[F].unit}
    )
    connect = pool.take(()).map(_.resource)
    interp = KleisliInterpreter[F](transactEC).ConnectionInterpreter
  } yield Transactor(
    (),
    {_: Unit => connect},
    interp,
    Strategy.default
  )
}