package coop.rchain.casper.util.rholang

import cats._
import cats.data.EitherT
import cats.effect.concurrent.MVar
import cats.effect.{Sync, _}
import cats.implicits._
import com.google.protobuf.ByteString
import coop.rchain.casper.ConstructDeploy
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.ProtoUtil
import coop.rchain.casper.util.rholang.RuntimeManager.StateHash
import coop.rchain.catscontrib.Catscontrib._
import coop.rchain.catscontrib.MonadTrans
import coop.rchain.crypto.codec.Base16
import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.models.Expr.ExprInstance.GString
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
  def storageRepr(hash: StateHash): F[Option[String]]
  def sequenceEffects(start: StateHash)(deploys: Seq[DeployData]): F[(StateHash, EvaluateResult)]
  def computeBonds(hash: StateHash): F[Seq[Bond]]
  def computeBalance(start: StateHash)(user: ByteString): F[Long]
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
        _                                        <- runtime.space.reset(Blake2b256Hash.fromByteArray(start.toByteArray))
        (codeHash, phloPrice, userId, timestamp) = ProtoUtil.getRholangDeployParams(deploy)
        _                                        <- runtime.shortLeashParams.setParams(codeHash, phloPrice, userId, timestamp)
        evaluateResult                           <- doInj(deploy, runtime.reducer, runtime.errorLog)(runtime.cost)
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

    val bondsQueryTerm = ConstructDeploy.sourceDeployNow(bondsQuery)
    withResetRuntime(hash) { runtime =>
      computeEffect(runtime)(bondsQueryTerm) *> getResult(runtime)()
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
  }

  def sequenceEffects(start: StateHash)(deploys: Seq[DeployData]): F[(StateHash, EvaluateResult)] =
    withRuntime(
      runtime =>
        deploys.toList
          .foldM(EvaluateResult(Cost(0), Vector.empty)) {
            case (EvaluateResult(cost0, errors0), deployData) =>
              computeEffect(runtime)(deployData).map {
                case EvaluateResult(cost1, errors1) =>
                  EvaluateResult(cost0 + cost1, errors0 |+| errors1)
              }
          }
          .product(
            runtime.space
              .createCheckpoint()
              .map(finish => ByteString.copyFrom(finish.root.bytes.toArray))
          )
          .map(_.swap)
    )

  def computeEffect(start: StateHash)(deploy: DeployData): F[EvaluateResult] =
    withResetRuntime(start)(computeEffect(_)(deploy))

  private[this] def computeEffect(runtime: Runtime[F])(deploy: DeployData): F[EvaluateResult] =
    for {
      runtimeParameters                        <- ProtoUtil.getRholangDeployParams(deploy).pure[F]
      (codeHash, phloPrice, userId, timestamp) = runtimeParameters
      _                                        <- runtime.shortLeashParams.setParams(codeHash, phloPrice, userId, timestamp)
      evaluateResult                           <- doInj(deploy, runtime.reducer, runtime.errorLog)(runtime.cost)
    } yield evaluateResult

  /** TODO: Investigate the requirements for handling errors thrown in the payment and balance query deploys. That is
    *errors existing in the "errors" component of the evaluation result returned by computeEffect. */
  private[this] def balanceQuerySource(user: ByteString): String =
    s"""
       | new stdout(`rho:io:stdout`), revAddressOps(`rho:rev:address`) in { new rl(`rho:registry:lookup`), revAddressOps(`rho:rev:address`), revVaultCh in {
       |   rl!(`rho:id:1o93uitkrjfubh43jt19owanuezhntag5wh74c6ur5feuotpi73q8z`, *revVaultCh) | stdout!("started-balanceQuery-findRevVaultInstance")
       |   | for (@(_, RevVault) <- revVaultCh) {
       |     stdout!("finished-balanceQuery-findRevVaultInstance") | new vaultCh, revAddressCh in {
       |        revAddressOps!("fromPublicKey", "${Base16.encode(user.toByteArray)}".hexToBytes(), *revAddressCh)
       |       | for(@revAddress <- revAddressCh) {
       |       @RevVault!("findOrCreate", revAddress, *vaultCh) | stdout!("started-balanceQuery-findUserRevVault")
       |       | for(@vaultEither <- vaultCh){
       |         match vaultEither {
       |           (true, vault) => {
       |             @vault!("balance", "__SCALA__") | stdout!("finished-balanceQuery-findUserRevVault-success")
       |           }
       |           (false, error) => {
       |             @"__SCALA__"!(error) | stdout!(error) | stdout!("finished-balanceQuery-findUserRevVault-failure")
       |           }
       |         }
       |       }
       |       }
       |     }
       |   }
       | }}
     """.stripMargin

  def computeBalance(start: StateHash)(user: ByteString): F[Long] =
    withRuntime(computeBalance(_)(user))

  private[this] def computeBalance(runtime: Runtime[F])(user: ByteString): F[Long] =
    Sync[F].delay(println("started-balanceQuery")) *> computeEffect(runtime)(
      ConstructDeploy.sourceDeployNow(balanceQuerySource(user))
    ) *> {
      getResult(runtime)() >>= {
        case Seq(RhoType.Number(balance)) =>
          Sync[F].delay(println(s"finished-computeUserBalance-success: balance = $balance")) *>
            balance.pure[F]
        case errors =>
          Sync[F].delay(
            println(
              s"finished-computeUserBalance-failure: errors = ${errors.map(RholangPrinter().buildString)}"
            )
          ) *>
            BugFoundError(
              s"Balance query failed unexpectedly: ${errors.map(RholangPrinter().buildString)}"
            ).raiseError[F, Long]
      }
    }

  private[this] def deployPaymentSource(amount: Long): String =
    s"""
       | new rl(`rho:registry:lookup`), poSCh, stdout(`rho:io:stdout`) in {
       |   rl!(`rho:id:cnec3pa8prp4out3yc8facon6grm3xbsotpd4ckjfx8ghuw77xadzt`, *poSCh) |
       |   for(@(_, PoS) <- poSCh) {
       |     @PoS!("pay", $amount, "__SCALA__")
       |   }
       | }
       """.stripMargin

  private[this] def computeDeployPayment(
      runtime: Runtime[F]
  )(user: ByteString, amount: Long): F[Unit] =
    Sync[F].delay(println(s"started-payForDeploy: amount = $amount")) *> computeEffect(runtime)(
      ConstructDeploy.sourceDeployNow(deployPaymentSource(amount)).withDeployer(user)
    ) *> {
      getResult(runtime)() >>= {
        case Seq(RhoType.Boolean(true)) =>
          Sync[F].delay(println(s"finished-payForDeploy-success")) *> ().pure[F]
        case Seq(RhoType.String(error)) =>
          Sync[F].delay(println(s"finished-payForDeploy-failed: error = $error")) *> BugFoundError(
            s"Deploy payment failed unexpectedly"
          ).raiseError[F, Unit]
        case other =>
          Sync[F].delay(println("finished-payForDeploy-failed: unknown error")) *> BugFoundError(
            s"Deploy payment returned unexpected result: ${other.map(RholangPrinter().buildString)}"
          ).raiseError[F, Unit]
      }
    }

  private[this] def withResetRuntime[A](start: StateHash)(f: Runtime[F] => F[A]): F[A] =
    withRuntime(
      runtime => runtime.space.reset(Blake2b256Hash.fromByteArray(start.toByteArray)) *> f(runtime)
    )

  private[this] def withRuntime[A](f: Runtime[F] => F[A]): F[A] =
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

  // TODO: Curry signature (start)(name).
  def getData(start: StateHash, name: Par): F[Seq[Par]] =
    withResetRuntime(start)(getData(_)(name))

  private[this] def getData(runtime: Runtime[F])(name: Par): F[Seq[Par]] =
    runtime.space.getData(name).map(_.flatMap(_.a.pars))

  private[this] def getResult(runtime: Runtime[F])(name: String = "__SCALA__"): F[Seq[Par]] =
    getData(runtime)(Par().withExprs(Seq(Expr(GString(name))))) <*
      computeEffect(runtime)(ConstructDeploy.sourceDeployNow(s"""for(_<-@"$name"){ Nil }"""))

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
  ): F[(StateHash, Seq[InternalProcessedDeploy])] =
    Concurrent[F]
      .defer {
        deploys.toList.foldLeftM(
          (Blake2b256Hash.fromByteArray(start.toByteArray), Seq.empty[InternalProcessedDeploy])
        ) {
          case ((prev, processedDeploys), deploy) =>
            import deploy._
            runtime.space.reset(prev) *> {
              computeBalance(runtime)(deployer) >>= { balance =>
                if (balance >= phloLimit * phloPrice) {
                  Sync[F].delay(println("started-evaluate-deployment")) *> computeEffect(runtime)(
                    deploy
                  ) >>= {
                    case EvaluateResult(cost, errors) =>
                      Sync[F].delay(
                        println(
                          s"finished-evaluate-deployment: cost = ${cost.value}, errors = ${errors.mkString(", ")}"
                        )
                      ) *>
                        errors.nonEmpty
                          .pure[F]
                          .ifM(ifTrue = runtime.space.reset(prev), ifFalse = ().pure[F]) *> {
                        computeDeployPayment(runtime)(deployer, cost.value * phloPrice) *> {
                          runtime.space.createCheckpoint().map { checkpoint =>
                            (
                              checkpoint.root,
                              processedDeploys :+ InternalProcessedDeploy(
                                deploy,
                                Cost.toProto(cost),
                                checkpoint.log,
                                DeployStatus.fromErrors(errors)
                              )
                            )
                          }
                        }
                      }
                  }
                } else {
                  Sync[F].delay(
                    println("finished-evaluate-deployment: aborted due to insufficient phlo")
                  ) *> (
                    prev,
                    processedDeploys :+ InternalProcessedDeploy(
                      deploy,
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

  // Replay: If status == OOPE, then balance is invalid. Re-verify user balance.
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
            val (codeHash, phloPrice, userId, timestamp) = ProtoUtil.getRholangDeployParams(deploy)
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
    Interpreter[F].injAttempt(
      reducer,
      errorLog,
      deploy.term,
      Cost(deploy.phloLimit)
    )
  }

  private[this] def setTimestamp(time: Option[Long], runtime: Runtime[F]): F[Unit] =
    time match {
      case Some(t) =>
        val timestamp: Par = Par(exprs = Seq(Expr(Expr.ExprInstance.GInt(t))))
        runtime.blockTime.setParams(timestamp)
      case None => ().pure[F]
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
      override def computeBalance(start: StateHash)(user: StateHash): T[F, Long] =
        runtimeManager.computeBalance(start)(user).liftM[T]
    }

  def eitherTRuntimeManager[E, F[_]: Monad](
      rm: RuntimeManager[F]
  ): RuntimeManager[EitherT[F, E, ?]] =
    RuntimeManager.forTrans[F, EitherT[?[_], E, ?]](rm)
}
