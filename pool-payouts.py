import argparse
import asyncio
import json
from jsonrpcclient import Ok, parse_json, request_json
import logging
from nimiqclient import *
import websockets

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
DEFAULT_LOG_LEVEL = "INFO"


class Range(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __eq__(self, other):
        return self.start <= other <= self.end


async def process_block(client, hash, pool_fee):
    logging.debug("Block hash: {}".format(hash))
    block = client.get_block_by_hash(hash, False)
    # Nothing to do when the block is a micro block
    if block.type == "micro":
        return
    logging.info("Running for block {}".format(block.number))
    validator_address = client.get_validator_address()
    validator = client.get_validator_by_address(
        validator_address, include_stakers=True)
    reward_account = client.get_account_by_address(validator.rewardAddress)
    total_staked_balance = 0
    # steak_porc = []
    # validator_balance = validator.balance
    for staker in validator.stakers:
        total_staked_balance += staker.balance
    for staker in validator.stakers:
        amount_to_send = int(
            float(staker.balance)/float(total_staked_balance) *
            float(reward_account.balance) * (1.0 - pool_fee))
        sender = reward_account.address
        recipient = staker.address
        if client.is_account_unlocked(reward_account.address):
            if sender != recipient:
                logging.info("Sending reward of {} to address {}".format(
                    amount_to_send, recipient))
                client.send_basic_transaction(
                    sender, recipient, amount_to_send, 0, block.number)
        else:
            raise InternalErrorException(
                "Can't send transaction because {0} is locked".format(
                    reward_account.address)
            )


async def run_client(uri, client, pool_fee):
    async with websockets.connect(uri) as ws:
        # call concat on the other side
        call_object = {
            "jsonrpc": "2.0",
            "method": "headSubscribe",
            "params": [],
            "id": 0,
        }
        await ws.send(json.dumps(call_object))
        # response = parse_json(await ws.recv())
        # print result
        # if isinstance(response, Ok):
        #    print(response)
        # else:
        #    print(response.message)
        result = parse_json(await ws.recv())
        subscription_id = result.result
        async for message in ws:
            # print(message)
            message = json.loads(message)
            if 'params' in message:
                if message['params']['subscription'] == subscription_id:
                    await process_block(client, message['params']['result'],
                                        pool_fee)


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
                        help="Private key of the validator reward account")
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

    # Create Nimiq RPC client
    client = NimiqClient(
        scheme="http", host=args.host, port=args.port
    )

    try:
        # Get consensus
        consensus = client.consensus()
        logging.info("Consensus: {0}".format(consensus))
    except InternalErrorException as error:
        logging.critical(
            "Got error when trying to connect to the RPC server: {0}".format(
                str(error)))

    # Import reward account private key
    client.importRawKey(args.private_key)
    # Get the validator this is running for
    validator_address = client.get_validator_address()
    validator = client.get_validator_by_address(
        validator_address, include_stakers=False)
    reward_account = client.get_account_by_address(validator.rewardAddress)
    client.unlock_account(reward_account.address)
    if not client.is_account_unlocked(reward_account.address):
        raise InternalErrorException(
            "Couldn't unlocked Validator reward address ({0}),"
            "perhaps an incorrect private key was specified?".format(
                reward_account.address)
        )
    if consensus:
        # client.head_subscribe(process_block)
        asyncio.get_event_loop().run_until_complete(
            run_client("ws://{}:{}/ws".format(args.host,
                       args.port), client, args.pool_fee)
        )


if __name__ == "__main__":
    main()
