package coop.rchain.casper.genesis.contracts

import cats.implicits._
import coop.rchain.crypto.PublicKey
import coop.rchain.models.Par
import coop.rchain.rholang.build.CompiledRholangSource
import coop.rchain.rholang.interpreter.{NormalizerEnv, ParBuilder}
import coop.rchain.rholang.interpreter.util.RevAddress
import monix.eval.Coeval

final case class RevGenerator(
    genesisPk: PublicKey,
    genesisAddress: RevAddress,
    userVaults: Seq[Vault],
    supply: Long
) extends CompiledRholangSource {

  val path: String = "<synthetic in Rev.scala>"

  val sourceVaults: String = userVaults
    .map {
      case Vault(revAddress, initialBalance) =>
        s"""("${revAddress.toBase58}", $initialBalance)"""
    }
    .mkString("[", ", ", "]")

  // From the initial mint amount we will subtract the total of the initial REV balance passed here.
  val code: String =
    s""" new rl(`rho:registry:lookup`), revVaultCh in {
       #   rl!(`rho:rchain:revVault`, *revVaultCh)
       #   | for (@(_, RevVault) <- revVaultCh) {
       #     @RevVault!("createGenesisVaults", $sourceVaults)
       #   }
       # }
     """.stripMargin('#')

  val normalizerEnv: NormalizerEnv = NormalizerEnv(deployId = none, deployerPk = genesisPk.some)

  val term: Par = ParBuilder[Coeval]
    .buildNormalizedTerm(code, normalizerEnv)
    .value()

}
