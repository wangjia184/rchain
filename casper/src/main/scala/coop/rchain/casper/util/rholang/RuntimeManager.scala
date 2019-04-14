package coop.rchain.casper.util.rholang

import cats._
import cats.data.EitherT
import cats.effect.concurrent.MVar
import cats.effect.{Sync, _}
import cats.implicits._
import com.google.protobuf.ByteString
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.ProtoUtil
import coop.rchain.casper.util.rholang.RuntimeManager.StateHash
import coop.rchain.catscontrib.Catscontrib._
import coop.rchain.catscontrib.MonadTrans
import coop.rchain.crypto.codec.Base16
import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.models.Expr.ExprInstance.{GBool, GString}
import coop.rchain.models._
import coop.rchain.rholang.interpreter.accounting._
import coop.rchain.rholang.interpreter.errors.BugFoundError
import coop.rchain.rholang.interpreter.storage.StoragePrinter
import coop.rchain.rholang.interpreter.{
  ChargingReducer,
  ErrorLog,
  EvaluateResult,
  Interpreter,
  RhoType,
  Runtime,
  accounting,
  PrettyPrinter => RholangPrinter
}
import coop.rchain.rspace.{Blake2b256Hash, ReplayException}

import scala.collection.immutable

trait RuntimeManager[F[_]] {
  def captureResults(
      start: StateHash,
      deploy: DeployData,
      name: String = "__SCALA__"
  ): F[Seq[Par]]
  def captureResults(start: StateHash, deploy: DeployData, name: Par): F[Seq[Par]]
  def replayComputeState(
      hash: StateHash,
      terms: Seq[InternalProcessedDeploy],
      time: Option[Long] = None
  ): F[Either[(Option[DeployData], Failed), StateHash]]
  def computeState(
      hash: StateHash,
      terms: Seq[DeployData],
      time: Option[Long] = None
  ): F[(StateHash, Seq[InternalProcessedDeploy])]
  def sequenceEffects(start: StateHash)(terms: Seq[DeployData]): F[(StateHash, EvaluateResult)]
  def storageRepr(hash: StateHash): F[Option[String]]
  def computeBonds(hash: StateHash): F[Seq[Bond]]
  def getData(hash: StateHash, channel: Par): F[Seq[Par]]
  def getContinuation(
      hash: StateHash,
      channels: immutable.Seq[Par]
  ): F[Seq[(Seq[BindPattern], Par)]]
  def emptyStateHash: StateHash
}

class RuntimeManagerImpl[F[_]: Concurrent] private[rholang] (
    val emptyStateHash: StateHash,
    runtimeContainer: MVar[F, Runtime[F]]
) extends RuntimeManager[F] {

  def captureResults(
      start: StateHash,
      deploy: DeployData,
      name: String = "__SCALA__"
  ): F[Seq[Par]] =
    captureResults(start, deploy, Par().withExprs(Seq(Expr(GString(name)))))

  def captureResults(start: StateHash, deploy: DeployData, name: Par): F[Seq[Par]] =
    Sync[F].bracket(runtimeContainer.take) { runtime =>
      for {
        _                                        <- Sync[F].delay(println("Before: CaptureResults.RSpaceReset"))
        _                                        <- runtime.space.reset(Blake2b256Hash.fromByteArray(start.toByteArray))
        _                                        <- Sync[F].delay(println("Before: CaptureResults.RSpaceReset"))
        (codeHash, phloPrice, userId, timestamp) = ProtoUtil.getRholangDeployParams(deploy)
        _                                        <- runtime.shortLeashParams.setParams(codeHash, phloPrice, userId, timestamp)
        _                                        <- Sync[F].delay(println("Before doInj"))
        evaluateResult                           <- doInj(deploy, runtime.reducer, runtime.errorLog)(runtime.cost)
        _                                        <- Sync[F].delay(println("After doInj"))
        values                                   <- runtime.space.getData(name).map(_.flatMap(_.a.pars))
        result                                   = if (evaluateResult.errors.nonEmpty) Seq.empty[Par] else values
      } yield result
    }(runtimeContainer.put)

  def replayComputeState(
      hash: StateHash,
      terms: Seq[InternalProcessedDeploy],
      time: Option[Long] = None
  ): F[Either[(Option[DeployData], Failed), StateHash]] =
    Sync[F].bracket(runtimeContainer.take) { runtime =>
      for {
        _      <- setTimestamp(time, runtime)
        result <- replayEval(terms, runtime, hash)
      } yield result
    }(runtimeContainer.put)

  def computeState(
      hash: StateHash,
      terms: Seq[DeployData],
      time: Option[Long] = None
  ): F[(StateHash, Seq[InternalProcessedDeploy])] =
    Sync[F].bracket(runtimeContainer.take) { runtime =>
      for {
        _      <- setTimestamp(time, runtime)
        result <- newEval(hash, terms, runtime)
      } yield result
    }(runtimeContainer.put)

  private def setTimestamp(
      time: Option[Long],
      runtime: Runtime[F]
  ): F[Unit] =
    time match {
      case Some(t) =>
        val timestamp: Par = Par(exprs = Seq(Expr(Expr.ExprInstance.GInt(t))))
        runtime.blockTime.setParams(timestamp)
      case None => ().pure[F]
    }

  def storageRepr(hash: StateHash): F[Option[String]] =
    Sync[F]
      .bracket(runtimeContainer.take) { runtime =>
        val blakeHash = Blake2b256Hash.fromByteArray(hash.toByteArray)
        runtime.space.reset(blakeHash).map(_ => StoragePrinter.prettyPrint(runtime.space))
      }(runtimeContainer.put)
      .attempt
      .map {
        case Right(print) => Some(print)
        case Left(_)      => None
      }

  def computeBonds(hash: StateHash): F[Seq[Bond]] = {
    val bondsQuery =
      """new rl(`rho:registry:lookup`), SystemInstancesCh, posCh in {
        |  rl!(`rho:id:wdwc36f4ixa6xacck3ddepmgueum7zueuczgthcqp6771kdu8jogm8`, *SystemInstancesCh) |
        |  for(@(_, SystemInstancesRegistry) <- SystemInstancesCh) {
        |    @SystemInstancesRegistry!("lookup", "pos", *posCh) |
        |    for(pos <- posCh){ pos!("getBonds", "__SCALA__") }
        |  }
        |}""".stripMargin

    val bondsQueryTerm = sourceDeploy(bondsQuery, 0L, accounting.MAX_VALUE)
    captureResults(hash, bondsQueryTerm)
      .ensureOr(
        bondsPar =>
          new IllegalArgumentException(
            s"Incorrect number of results from query of current bonds: ${bondsPar.size}"
          )
      )(bondsPar => bondsPar.size == 1)
      .map { bondsPar =>
        toBondSeq(bondsPar.head)
      }
  }

  def sequenceEffects(start: StateHash)(deploys: Seq[DeployData]): F[(StateHash, EvaluateResult)] =
    withRuntime(
      runtime =>
        deploys.toList
          .foldM(
            (Blake2b256Hash.fromByteArray(start.toByteArray), EvaluateResult(Cost(0), Vector.empty))
          )(foldEffect(runtime))
          .map(_.leftMap(finish => ByteString.copyFrom(finish.bytes.toArray)))
    )

  private def foldEffect(
      runtime: Runtime[F]
  ): ((Blake2b256Hash, EvaluateResult), DeployData) => F[(Blake2b256Hash, EvaluateResult)] = {
    case ((start, EvaluateResult(cost0, errors0)), deploy) =>
      for {
        _                                        <- Sync[F].delay(println("started-foldEffect"))
        _                                        <- runtime.space.reset(start)
        (codeHash, phloPrice, userId, timestamp) = ProtoUtil.getRholangDeployParams(deploy)
        _                                        <- runtime.shortLeashParams.setParams(codeHash, phloPrice, userId, timestamp)
        evaluateResult                           <- doInj(deploy, runtime.reducer, runtime.errorLog)(runtime.cost)
        EvaluateResult(cost1, errors1)           = evaluateResult
        finish <- if (errors1.isEmpty) runtime.space.createCheckpoint().map(_.root)
                 else start.pure[F]
      } yield (finish, EvaluateResult(cost0 + cost1, errors0 |+| errors1))
  }

  private def withResetRuntime[A](start: StateHash)(f: Runtime[F] => F[A]): F[A] =
    withRuntime(
      runtime => runtime.space.reset(Blake2b256Hash.fromByteArray(start.toByteArray)) *> f(runtime)
    )

  private def withRuntime[A](f: Runtime[F] => F[A]): F[A] =
    Sync[F].bracket(runtimeContainer.take)(f)(runtimeContainer.put)

  private def toBondSeq(bondsMap: Par): Seq[Bond] =
    bondsMap.exprs.head.getEMapBody.ps.map {
      case (validator: Par, bond: Par) =>
        assert(validator.exprs.length == 1, "Validator in bonds map wasn't a single string.")
        assert(bond.exprs.length == 1, "Stake in bonds map wasn't a single integer.")
        val validatorName = validator.exprs.head.getGByteArray
        val stakeAmount   = bond.exprs.head.getETupleBody.ps.head.exprs.head.getGInt
        Bond(validatorName, stakeAmount)
    }.toList

  private def sourceDeploy(source: String, timestamp: Long, phlos: Long): DeployData =
    DeployData(
      deployer = ByteString.EMPTY,
      timestamp = timestamp,
      term = source,
      phloLimit = phlos
    )

  def getData(hash: StateHash, channel: Par): F[Seq[Par]] =
    withResetRuntime(hash)(_.space.getData(channel).map(_.flatMap(_.a.pars)))

  def getContinuation(
      hash: StateHash,
      channels: immutable.Seq[Par]
  ): F[Seq[(Seq[BindPattern], Par)]] =
    withResetRuntime(hash)(
      runtime =>
        runtime.space
          .getWaitingContinuations(channels)
          .map(
            results =>
              results
                .filter(_.continuation.taggedCont.isParBody)
                .map(result => (result.patterns, result.continuation.taggedCont.parBody.get.body))
          )
    )

  private def newEval(
      start: StateHash,
      deploys: Seq[DeployData],
      runtime: Runtime[F]
  ): F[(StateHash, Seq[InternalProcessedDeploy])] = {

    def computeUserBalance(start: Blake2b256Hash, user: ByteString): F[Long] = {

      val balanceQuerySource: String =
        s"""
           | new stdout(`rho:io:stdout`), revAddressOps(`rho:rev:address`) in { stdout!("BalanceQuery") | new rl(`rho:registry:lookup`), revAddressOps(`rho:rev:address`), revVaultCh in {
           |   rl!(`rho:id:1o93uitkrjfubh43jt19owanuezhntag5wh74c6ur5feuotpi73q8z`, *revVaultCh) | stdout!("BalanceQuery.RevVaultInstanceQuery")
           |   | for (@(_, RevVault) <- revVaultCh) {
           |     stdout!("BalanceQuery.RevVaultInstanceFound") | new vaultCh, revAddressCh in {
           |        revAddressOps!("fromPublicKey", "${Base16.encode(user.toByteArray)}".hexToBytes(), *revAddressCh)
           |       | for(@revAddress <- revAddressCh) {
           |       @RevVault!("findOrCreate", revAddress, *vaultCh) | stdout!("BalanceQuery.UserRevVaultQuery")
           |       | for(@vaultEither <- vaultCh){
           |         stdout!("BalanceQuery.UserRevVaultEitherFound") | match vaultEither {
           |           (true, vault) => {
           |             @vault!("balance", "__SCALA__") | stdout!("BalanceQuery.UserRevVaultFound")
           |           }
           |           (false, error) => {
           |             @"__SCALA__"!(error) | stdout!(error)
           |           }
           |         }
           |       }
           |       }
           |     }
           |   }
           | }}
     """.stripMargin

      Sync[F].delay(println("started-computeUserBalance")) *>
        foldEffect(runtime)(
          (start, EvaluateResult(Cost(0), Vector.empty)),
          DeployData(
            deployer = user,
            term = balanceQuerySource,
            phloLimit = Long.MaxValue,
            timestamp = System.currentTimeMillis()
          )
        ).flatMap { _ =>
          runtime.space
            .getData(Par().withExprs(Seq(Expr(GString("__SCALA__")))))
            .map(_.flatMap(_.a.pars)) >>= {
            case Seq(RhoType.Number(balance)) =>
              Sync[F].delay(println(s"finished-computeUserBalance: $balance")) *> balance.pure[F]
            case errors =>
              Sync[F].delay(
                println(
                  s"finished-computeUserBalance: ${errors.map(RholangPrinter().buildString)}"
                )
              ) *>
                BugFoundError(
                  s"Balance query failed unexpectedly: ${errors.map(RholangPrinter().buildString)}"
                ).raiseError[F, Long]
          }
        }
    }

    /**
      * FIXME: Since "__SCALA__" is a public name, the result of this code can be
      *        intercepted and/or forged. Require a more general method to return
      *        results to an unforgeable name known only by the runtime manager.
      *        Or, perhaps clear the "__SCALA__" channel after each evaluation.
      *
      * @note This function assumes that PoS.pay always halts. This justifies the maximum
      *       value phlo limit. It also assumes that all deploys are valid at this stage
      *       of execution, such that PoS.pay should always succeed.
      *
      */
    def payForDeploy(
        start: Blake2b256Hash,
        user: ByteString,
        phloPrice: Long,
        phloLimit: Long
    ): F[Unit] = {

      val payDeploySource: String =
        s"""
           | new rl(`rho:registry:lookup`), poSCh, stdout(`rho:io:stdout`) in {
           |   rl!(`rho:id:cnec3pa8prp4out3yc8facon6grm3xbsotpd4ckjfx8ghuw77xadzt`, *poSCh) |
           |   for(@(_, PoS) <- poSCh) {
           |     @PoS!("pay", ${phloPrice * phloLimit}, "__SCALA__") | stdout!("started-PoS.pay")
           |   }
           | }
       """.stripMargin

      Sync[F].delay(println("started-payForDeploy")) *>
        foldEffect(runtime)(
          (start, EvaluateResult(Cost(0), Vector.empty)),
          DeployData(
            deployer = user,
            term = payDeploySource,
            phloLimit = Long.MaxValue,
            timestamp = System.currentTimeMillis()
          )
        ) >>= {
        case (finish, EvaluateResult(cost, errors)) =>
          Sync[F].delay(println(s"payForDeployCost: $cost")) *> Sync[F].delay(
            println(s"payForDeployErrors: $errors")
          ) *>
            runtime.space
              .getData(Par().withExprs(Seq(Expr(GString("__SCALA__")))))
              .map(_.flatMap(_.a.pars)) >>= {
            case Seq(RhoType.Number(amount)) if amount == (phloPrice * phloLimit) =>
              Sync[F].delay(println("finished-payForDeploy: success")) *> ().pure[F]
            case Seq(RhoType.Number(amount)) if amount == 0 =>
              Sync[F].delay(println("finished-payForDeploy: failed")) *> BugFoundError(
                s"Deploy payment failed unexpectedly"
              ).raiseError[F, Unit]
            case errors =>
              Sync[F].delay(println("finished-payForDeploy: failed")) *> BugFoundError(
                s"Deploy payment returned unexpected result: ${errors.map(RholangPrinter().buildString)}"
              ).raiseError[F, Unit]
          }

      }
    }

    Concurrent[F]
      .defer {
        deploys.toList.foldM(
          (Blake2b256Hash.fromByteArray(start.toByteArray), Seq.empty[InternalProcessedDeploy])
        ) {
          case ((prev, processedDeploys), deployData) =>
            import deployData._
            runtime.space.reset(prev) *>
              computeUserBalance(prev, deployer) >>= { balance =>
              runtime.space.reset(prev) *> {
                if (balance >= phloLimit * phloPrice) {
                  for {
                    executionParameters                      <- ProtoUtil.getRholangDeployParams(deployData).pure[F]
                    (codeHash, phloPrice, userId, timestamp) = executionParameters
                    _ <- runtime.shortLeashParams.setParams(
                          codeHash,
                          phloPrice,
                          userId,
                          timestamp
                        )
                    evaluateResult <- doInj(deployData, runtime.reducer, runtime.errorLog)(
                                       runtime.cost
                                     )
                    EvaluateResult(cost, errors) = evaluateResult
                    _ <- if (errors.nonEmpty)
                          runtime.space.reset(prev) *>
                            payForDeploy(prev, deployer, 0L, Long.MaxValue)
                        else
                          runtime.space.createCheckpoint() >>= { checkpoint0 =>
                            payForDeploy(checkpoint0.root, deployer, 0L, Long.MaxValue)
                          }
                    checkpoint1 <- runtime.space.createCheckpoint()
                    result = (
                      checkpoint1.root,
                      processedDeploys :+
                        InternalProcessedDeploy(
                          deployData,
                          Cost.toProto(cost),
                          checkpoint1.log,
                          DeployStatus.fromErrors(errors)
                        )
                    )
                  } yield result
                } else {
                  (
                    prev,
                    processedDeploys :+ InternalProcessedDeploy(
                      deployData,
                      Cost.toProto(Cost(0)),
                      Seq.empty,
                      UserErrors(
                        Vector(
                          new IllegalArgumentException(
                            s"Deployer REV balance must be greater than ${phloLimit * phloPrice}"
                          )
                        )
                      )
                    )
                  ).pure[F]
                }
              }
            }
        }
      }
      .map(_.leftMap(finish => ByteString.copyFrom(finish.bytes.toArray)))
  }

  private def replayEval(
      terms: Seq[InternalProcessedDeploy],
      runtime: Runtime[F],
      initHash: StateHash
  ): F[Either[(Option[DeployData], Failed), StateHash]] = {

    def doReplayEval(
        terms: Seq[InternalProcessedDeploy],
        hash: Blake2b256Hash
    ): F[Either[(Option[DeployData], Failed), StateHash]] =
      Concurrent[F].defer {
        terms match {
          case InternalProcessedDeploy(deploy, _, log, status) +: rem =>
            val (codeHash, phloPrice, userId, timestamp) = ProtoUtil.getRholangDeployParams(
              deploy
            )
            for {
              _         <- runtime.shortLeashParams.setParams(codeHash, phloPrice, userId, timestamp)
              _         <- runtime.replaySpace.rig(hash, log.toList)
              injResult <- doInj(deploy, runtime.replayReducer, runtime.errorLog)(runtime.cost)
              //TODO: compare replay deploy cost to given deploy cost
              EvaluateResult(cost, errors) = injResult
              cont <- DeployStatus.fromErrors(errors) match {
                       case int: InternalErrors => Left(Some(deploy) -> int).pure[F]
                       case replayStatus =>
                         if (status.isFailed != replayStatus.isFailed)
                           Left(Some(deploy) -> ReplayStatusMismatch(replayStatus, status)).pure[F]
                         else if (errors.nonEmpty) doReplayEval(rem, hash)
                         else {
                           runtime.replaySpace
                             .createCheckpoint()
                             .attempt
                             .flatMap {
                               case Right(newCheckpoint) =>
                                 doReplayEval(rem, newCheckpoint.root)
                               case Left(ex: ReplayException) =>
                                 Either
                                   .left[(Option[DeployData], Failed), StateHash](
                                     none[DeployData] -> UnusedCommEvent(ex)
                                   )
                                   .pure[F]
                               case Left(ex) =>
                                 Either
                                   .left[(Option[DeployData], Failed), StateHash](
                                     none[DeployData] -> UserErrors(Vector(ex))
                                   )
                                   .pure[F]
                             }
                         }
                     }
            } yield cont

          case _ =>
            Either
              .right[(Option[DeployData], Failed), StateHash](
                ByteString.copyFrom(hash.bytes.toArray)
              )
              .pure[F]
        }
      }

    doReplayEval(terms, Blake2b256Hash.fromByteArray(initHash.toByteArray))
  }

  private[this] def doInj(
      deploy: DeployData,
      reducer: ChargingReducer[F],
      errorLog: ErrorLog[F]
  )(implicit C: _cost[F]) = {
    implicit val rand: Blake2b512Random = Blake2b512Random(
      DeployData.toByteArray(ProtoUtil.stripDeployData(deploy))
    )
    Sync[F].delay(println("started-doInj")) *> Interpreter[F].injAttempt(
      reducer,
      errorLog,
      deploy.term,
      Cost(deploy.phloLimit)
    ) <* Sync[F].delay(println("finished-doInj"))
  }

}

object RuntimeManager {
  type StateHash = ByteString

  def fromRuntime[F[_]: Concurrent: Sync](
      active: Runtime[F]
  ): F[RuntimeManager[F]] =
    for {
      _                <- active.space.clear()
      _                <- active.replaySpace.clear()
      _                <- Runtime.injectEmptyRegistryRoot(active.space, active.replaySpace)
      checkpoint       <- active.space.createCheckpoint()
      replayCheckpoint <- active.replaySpace.createCheckpoint()
      hash             = ByteString.copyFrom(checkpoint.root.bytes.toArray)
      replayHash       = ByteString.copyFrom(replayCheckpoint.root.bytes.toArray)
      _                = assert(hash == replayHash)
      runtime          <- MVar[F].of(active)
    } yield new RuntimeManagerImpl(hash, runtime)

  def forTrans[F[_]: Monad, T[_[_], _]: MonadTrans](
      runtimeManager: RuntimeManager[F]
  ): RuntimeManager[T[F, ?]] =
    new RuntimeManager[T[F, ?]] {
      override def captureResults(
          start: RuntimeManager.StateHash,
          deploy: DeployData,
          name: String
      ): T[F, Seq[Par]] = runtimeManager.captureResults(start, deploy, name).liftM[T]

      override def captureResults(
          start: RuntimeManager.StateHash,
          deploy: DeployData,
          name: Par
      ): T[F, Seq[Par]] = runtimeManager.captureResults(start, deploy, name).liftM[T]

      override def replayComputeState(
          hash: RuntimeManager.StateHash,
          terms: scala.Seq[InternalProcessedDeploy],
          time: Option[Long]
      ): T[F, scala.Either[(Option[DeployData], Failed), RuntimeManager.StateHash]] =
        runtimeManager.replayComputeState(hash, terms, time).liftM[T]

      override def computeState(
          hash: RuntimeManager.StateHash,
          terms: scala.Seq[DeployData],
          time: Option[Long]
      ): T[F, (RuntimeManager.StateHash, scala.Seq[InternalProcessedDeploy])] =
        runtimeManager.computeState(hash, terms, time).liftM[T]

      override def sequenceEffects(
          start: StateHash
      )(terms: Seq[DeployData]): T[F, (StateHash, EvaluateResult)] =
        runtimeManager.sequenceEffects(start)(terms).liftM[T]

      override def storageRepr(
          hash: RuntimeManager.StateHash
      ): T[F, Option[String]] = runtimeManager.storageRepr(hash).liftM[T]

      override def computeBonds(
          hash: RuntimeManager.StateHash
      ): T[F, scala.Seq[Bond]] = runtimeManager.computeBonds(hash).liftM[T]

      override def getData(
          hash: StateHash,
          channel: Par
      ): T[F, scala.Seq[Par]] = runtimeManager.getData(hash, channel).liftM[T]

      override def getContinuation(
          hash: StateHash,
          channels: scala.collection.immutable.Seq[Par]
      ): T[F, scala.Seq[(scala.Seq[BindPattern], Par)]] =
        runtimeManager.getContinuation(hash, channels).liftM[T]

      override val emptyStateHash: StateHash = runtimeManager.emptyStateHash

    }

  def eitherTRuntimeManager[E, F[_]: Monad](
      rm: RuntimeManager[F]
  ): RuntimeManager[EitherT[F, E, ?]] =
    RuntimeManager.forTrans[F, EitherT[?[_], E, ?]](rm)
}
