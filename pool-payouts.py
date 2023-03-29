import argparse
import asyncio
import logging
import time
from nimiqclient import *

from payments import Payments

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
DEFAULT_LOG_LEVEL = "INFO"

class Range(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __eq__(self, other):
        return self.start <= other <= self.end


async def process_logs(client, log, kwargs):
    block_number = log.metadata.blockNumber
    log = log.data
    logging.debug("Received log of type: {}".format(log.type))
    if log.type != "applied-block":
        return

    # The obtained log is of type BlockLog, we have to iterate its inherents
    for inherent in log.inherentLogs:
        if inherent.type != "payout-reward":
            continue

        logging.info("Running for block {}".format(block_number))
        validator_address = await client.get_validator_address()
        stakers = (await client.get_stakers_by_validator_address(
            validator_address)).data
        total_staked_balance = 0
        payments = kwargs['payments']
        sender = kwargs['reward_address']

        for staker in stakers:
            total_staked_balance += staker.balance
        for staker in stakers:
            amount_to_send = int(
                float(staker.balance)/float(total_staked_balance) *
                float(inherent.value) * (1.0 - kwargs['pool_fee']))
            # If we don't have anything to pay, skip this staker
            if amount_to_send == 0 or sender == staker.address:
                continue
            logging.debug(
                "Registering payment to '{}' for an amount of {}".format(
                    staker.address, amount_to_send))
            await payments.register_payment(staker.address, amount_to_send)


async def run_client(host, port, private_key, pool_fee, use_stake_txns,
                     frequency):
    async with NimiqClient(
        scheme="ws", host=host, port=port
    ) as client:
        try:
            consensus = False
            while not consensus:
                # Get consensus
                consensus = await client.consensus()
                logging.info("Consensus: {0}".format(consensus))
                time.sleep(1)
        except InternalErrorException as error:
            logging.critical(
                "Got error when trying to connect to the RPC server: {0}"
                .format(str(error)))

        # Import reward account private key
        await client.importRawKey(private_key)
        # Get the validator this is running for
        validator_address = await client.get_validator_address()
        validator = (await client.get_validator_by_address(
            validator_address)).data
        # Get reward account
        reward_account = (await client.get_account_by_address(
            validator.rewardAddress)).data
        await client.unlock_account(reward_account.address)
        if not await client.is_account_unlocked(reward_account.address):
            raise InternalErrorException(
                "Couldn't unlocked Validator reward address ({0}),"
                "perhaps an incorrect private key was specified?".format(
                    reward_account.address)
            )

        payments = Payments()

        # Subscribe to log of type payout rewards that contains the
        # reward address
        await client.subscribe_for_logs_by_addresses_and_types(
            [validator.rewardAddress],
            [LogType.PAYOUT_REWARD],
            process_logs,
            pool_fee=pool_fee,
            payments=payments,
            reward_address=reward_account.address)
        while True:
            await asyncio.sleep(frequency)
            await payments.process_payments(client, reward_account.address,
                                            use_stake_txns)


def parse_args():
    """
    Parse command line arguments:
    - RPC host
    - RPC port
    - Validator reward account private key
    - Pool fee as float [0, 1]

    :return The parsed command line arguments.
    :rtype: Namespace
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('-H', '--host', type=str, required=True,
                        help="RPC host for the Nimiq client RPC connection")
    parser.add_argument('-P', '--port', type=int, required=True,
                        help="RPC port for the Nimiq client RPC connection")
    parser.add_argument('-pk', "--private-key", type=str, required=True,
                        help="Private key of the validator reward account")
    parser.add_argument('-pf', "--pool-fee", type=float, required=True,
                        choices=[Range(0.0, 1.0)],
                        help=("Pool fee to apply when sending rewards to "
                              "stakers. Value must be a float between 0 and "
                              "1"))
    parser.add_argument('-st', "--stake-transactions", action="store_true",
                        help=("Set this to use staking transactions instead "
                              "of regular (basic) transactions for paying "
                              "rewards"))
    parser.add_argument('-f', "--frequency", type=int, required=True,
                        help="Payments frequency in seconds")
    parser.add_argument("--verbose", "-v", dest="log_level",
                        action="append_const", const=-1)
    return parser.parse_args()


def setup_logging(args):
    """
    Sets-up logging according to the arguments received.

    :params args: Command line arguments of the program
    :type args: Namespace
    """
    # Adjust log level accordingly
    log_level = LOG_LEVELS.index(DEFAULT_LOG_LEVEL)
    for adjustment in args.log_level or ():
        log_level = min(len(LOG_LEVELS) - 1, max(log_level + adjustment, 0))

    log_level_name = LOG_LEVELS[log_level]
    logging.getLogger().setLevel(log_level_name)
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')


def main():
    # Parse arguments
    args = parse_args()

    # Setup logging
    setup_logging(args)

    asyncio.get_event_loop().run_until_complete(
        run_client(args.host, args.port, args.private_key,
                   args.pool_fee, args.stake_transactions, args.frequency)
    )


if __name__ == "__main__":
    main()
