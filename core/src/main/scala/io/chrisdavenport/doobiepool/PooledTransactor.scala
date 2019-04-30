package io.chrisdavenport.doobiepool

import cats.implicits._
import cats.effect._
// import cats.effect.concurrent._
import doobie.KleisliInterpreter
import doobie.util.transactor.Transactor
import doobie.util.transactor.Strategy
import scala.concurrent.ExecutionContext
import io.chrisdavenport.keypool._

import java.sql.{Connection, DriverManager}
import cats.effect.concurrent.Semaphore

object PooledTransactor {

  /**
   * Create a Transactor
   */
  def apply[F[_]: Concurrent: ContextShift](
    pool: KeyPool[F, Unit, Connection],
    transactEC: ExecutionContext,
    maxConnectionsActive: Int // Should this be Int with an Effect or Take a Semaphore Explicitly? Starting with
    // int is the more reserved choice.
  ): F[Transactor[F]] = 
    Semaphore[F](maxConnectionsActive.toLong).map( sem =>
      Transactor(
        pool,
        {p: KeyPool[F, Unit, Connection] =>
          Resource.make(sem.acquire)(_ => sem.release) >>
          p.take(()).map(_.resource)
        },
        KleisliInterpreter[F](transactEC).ConnectionInterpreter,
        Strategy.default
      )
    )

  private def create[F[_]: Concurrent: Timer : ContextShift](
    driver: String,
    conn: () => Connection,
    maxConnectionsInPool: Int,
    connectEC:  ExecutionContext
  ): Resource[F, KeyPool[F, Unit, Connection]] = KeyPool.create[F, Unit, Connection](
    {_: Unit =>
      ContextShift[F].evalOn(connectEC)(
        Sync[F].delay { Class.forName(driver); conn() }
      )},
    { case (_, conn) =>
      ContextShift[F].evalOn(connectEC)(Sync[F].delay { conn.close() })},
    Reuse,
    Long.MaxValue,
    maxConnectionsInPool,
    maxConnectionsInPool,
    {_: Throwable => Sync[F].unit}
  )

  def pool[F[_]: Concurrent: Timer: ContextShift](
    driver: String,
    url:    String,
    maxConnectionsInPool: Int,
    connectEC:  ExecutionContext
  ): Resource[F, KeyPool[F, Unit, Connection]] =
    create[F](
      driver,
      () => DriverManager.getConnection(url),
      maxConnectionsInPool,
      connectEC
    )

  def pool[F[_]: Concurrent: Timer: ContextShift](
    driver: String,
    url:    String,
    user:   String,
    pass: String,
    maxConnectionsInPool: Int,
    connectEC:  ExecutionContext
  ): Resource[F, KeyPool[F, Unit, Connection]] =
    create[F](
      driver,
      () => DriverManager.getConnection(url, user, pass),
      maxConnectionsInPool,
      connectEC
    )

  def pool[F[_]: Concurrent: Timer: ContextShift](
    driver: String,
    url:    String,
    info: java.util.Properties,
    maxConnectionsInPool: Int,
    connectEC:  ExecutionContext
  ): Resource[F, KeyPool[F, Unit, Connection]] =
    create[F](
      driver,
      () => DriverManager.getConnection(url, info),
      maxConnectionsInPool,
      connectEC
    )

}