package coop.rchain.casper.genesis.contracts
import coop.rchain.casper.helper.RhoSpec
import coop.rchain.rholang.SuccessfulResultCollectorTest
import scala.concurrent.duration._

class SuccessfulResultCollectorSpec
    extends RhoSpec(SuccessfulResultCollectorTest, Seq.empty, 10.seconds)
