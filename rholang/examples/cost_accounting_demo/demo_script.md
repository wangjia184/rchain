# SETUP

    #execute from the `examples` directory
    . keys.env

# DEMO START

In this example, we will act as the genesis user. That is, the user corresponding to the genesis vault.

A deployment should result in a deduction of REV (= phlo consumed * phlo price) from the vault of the deployer. Here's how
you would check your vault balance:

    ./propose.sh $1000000 $0 $GENESIS_PRV vault_demo/2.check_balance.rho "-e s/%REV_ADDR/$GENESIS_REV/"
    
Since the phlo price was set to zero, your balance after the deploy should be equivalent to your balance before the
deploy:

    ./propose.sh $1000000 $1 $GENESIS_PRV vault_demo/2.check_balance.rho "-e s/%REV_ADDR/$GENESIS_REV/"

Since the phlo price was non-zero, your balance after the deploy should be less than your balance before the deploy,

    ./propose.sh $1000000 $0 $GENESIS_PRV vault_demo/2.check_balance.rho "-e s/%REV_ADDR/$GENESIS_REV/"
    
and the REV deducted from your vault should be added to the Casper vault.

    ./propose.sh $1000000 $0 $GENESIS_PRV vault_demo/2.check_balance.rho "-e s/%REV_ADDR/$CASPER_REV/"
    
*Future releases will allow node operators to define acceptable minimum and maximum phlo prices.*

Let's transfer a few REV from you to Alice:

    ./propose.sh $1000000 $0 $GENESIS_PRV vault_demo/3.transfer_funds.rho "-e s/%FROM/$GENESIS_REV/ -e s/%TO/$ALICE_REV/"

If Alice issues a deployment with a maximum cost greater than her REV balance, the deployment will be rejected by the node.

    ./propose.sh $101 $1 $ALICE_PRV vault_demo/3.transfer_funds.rho "-e s/%FROM/$ALICE_REV/ -e s/%TO/$GENESIS_REV/"
    
This, however, will not affect Alice's REV balance.

    ./propose.sh $1000000 $0 $GENESIS_PRV vault_demo/2.check_balance.rho "-e s/%REV_ADDR/$ALICE_REV/"
    
On the other hand, if Alice deploys a contract that reaches an error-state (e.g. OutOfPhloError),

    ./propose.sh $1000000 $1 $ALICE_PRV cost_accounting_demo/0.evil_contract.rho
    
then her REV balance will be reduced by the cost of the contract up to the point it threw the error,

    ./propose.sh $1000000 $0 $GENESIS_PRV vault_demo/2.check_balance.rho "-e s/%REV_ADDR/$ALICE_REV/"
    
and the effects of her contract will be discarded.

    ./propose.sh $1000000 $0 $GENESIS_PRV cost_accounting_demo/1.evil_contract_witness.rho
        