package io.chrisdavenport.doobiepool

import cats.effect._
// import cats.effect.implicits._
// import cats.implicits._
// import doobie._
import doobie.implicits._
import org.specs2.mutable.Specification
import scala.concurrent.ExecutionContext

class PooledTransactorSpec extends Specification {

  val q = sql"select 42".query[Int].unique

  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  def xa[A[_]: Concurrent: Timer: ContextShift] = PooledTransactor[A](
    "org.h2.Driver",
    "jdbc:h2:mem:queryspec;DB_CLOSE_DELAY=-1",
    "sa", 
    "",
    10,
    ExecutionContext.global,
    ExecutionContext.global
  )

  "transactor" should {

    "support cats.effect.IO" in {
      xa[IO].use(q.transact(_)).unsafeRunSync must_=== 42
    }

  }

//   @SuppressWarnings(Array("org.wartremover.warts.Var"))
//   class ConnectionTracker {
//     var connections = List.empty[java.sql.Connection]

//     def track[F[_]: Async: ContextShift](xa: Transactor[F]) = {
//       def withA(t: doobie.util.transactor.Transactor[F]): Transactor.Aux[F, t.A] = {
//         Transactor.connect.modify(t, f => a => {
//           f(a).map { conn =>
//             connections = conn :: connections
//             conn
//           }
//         })
//       }
//       withA(xa)
//     }
//   }

//   "Connection lifecycle" >> {

//     "Connection.close should be called on success" in {
//       val tracker = new ConnectionTracker
//       val transactor = tracker.track(xa[IO])
//       sql"select 1".query[Int].unique.transact(transactor).unsafeRunSync
//       tracker.connections.map(_.isClosed) must_== List(true)
//     }

//     "Connection.close should be called on failure" in {
//       val tracker = new ConnectionTracker
//       val transactor = tracker.track(xa[IO])
//       sql"abc".query[Int].unique.transact(transactor).attempt.unsafeRunSync.toOption must_== None
//       tracker.connections.map(_.isClosed) must_== List(true)
//     }

//   }

//   "Connection lifecycle (streaming)" >> {

//     "Connection.close should be called on success" in {
//       val tracker = new ConnectionTracker
//       val transactor = tracker.track(xa[IO])
//       sql"select 1".query[Int].stream.compile.toList.transact(transactor).unsafeRunSync
//       tracker.connections.map(_.isClosed) must_== List(true)
//     }

//     "Connection.close should be called on failure" in {
//       val tracker = new ConnectionTracker
//       val transactor = tracker.track(xa[IO])
//       sql"abc".query[Int].stream.compile.toList.transact(transactor).attempt.unsafeRunSync.toOption must_== None
//       tracker.connections.map(_.isClosed) must_== List(true)
//     }

// }
}