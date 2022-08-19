import asyncio
import logging
from nimiqclient import *

VALIDITY_START_WINDOW_OFFSET = "+0"


class Payments:

    def __init__(self):
        self.payments = {}
        self.lock = asyncio.Lock()

    async def register_payment(self, address, amount):
        async with self.lock:
            if address in self.payments:
                self.payments[address] += amount
            else:
                self.payments[address] = amount

    async def process_payments(self, client, sender, use_stake_txns):
        if not await client.is_account_unlocked(sender):
            raise InternalErrorException(
                "Can't send transaction because {0} is locked".format(
                    sender)
            )
        async with self.lock:
            logging.info("Processing payments")
            while len(self.payments) != 0:
                (recipient, amount) = self.payments.popitem()
                logging.info("Sending reward of {} to address '{}'".format(
                    amount, recipient))
                if use_stake_txns:
                    await client.send_stake_transaction(
                        sender, recipient, amount, 0,
                        VALIDITY_START_WINDOW_OFFSET)
                else:
                    await client.send_basic_transaction(
                        sender, recipient, amount, 0,
                        VALIDITY_START_WINDOW_OFFSET)
