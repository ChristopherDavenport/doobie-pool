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
    maxConnectionsActive: Int,
    transactEC: ExecutionContext
  ): F[Transactor[F]] =
    Semaphore[F](maxConnectionsActive.toLong)
      .map(impl(pool, _, transactEC))

  /**
   * Create a Transactor with an Externally Inspectable/Controllable Semaphore
   */
  def impl[F[_]: Concurrent: ContextShift](
    pool: KeyPool[F, Unit, Connection],
    activeConnections: Semaphore[F],
    transactEC: ExecutionContext
  ): Transactor[F] = Transactor(
    pool,
    {p: KeyPool[F, Unit, Connection] =>
      Resource.make(activeConnections.acquire)(_ => activeConnections.release) >>
      p.take(()).map(_.resource)
    },
    KleisliInterpreter[F](transactEC).ConnectionInterpreter,
    Strategy.default
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

  /**
   * Build a Pool of Connections, using the url.
   */
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

  /**
   * Build a Pool of Connections, using the url, user, and password.
   */
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

  /**
   * Build a Pool of Connections, using the url and connection info.
   */
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