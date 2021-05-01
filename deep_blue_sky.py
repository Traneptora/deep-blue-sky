#!/usr/bin/env python3

import asyncio, signal
import os, re, sys, time
import traceback
import urllib.request
import contextlib, logging

import discord
from discord.ext import tasks

class DeepBlueSky(discord.Client):

    # setup stuff

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('discord')
        self.log_file = open('bot_output.log', mode='a', buffering=1, encoding='UTF-8')
        handler = logging.StreamHandler(stream=self.log_file)
        formatter = logging.Formatter(fmt='[{asctime}] {levelname}: {message}', style='{')
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
   
        discord.Client.__init__(self, *args, **kwargs)
        for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]:
            self.loop.add_signal_handler(sig, lambda sig = sig: asyncio.create_task(self.signal_handler(sig, self.loop)))

    # cleanup stuff

    async def cleanup(self):
        self.logger.info('Received signal, exiting gracefully')
        await self.change_presence(status=discord.Status.invisible, activity=None)
        await self.close()

    async def signal_handler(self, signal, frame):
        try:
            await self.cleanup()
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks)
        finally:
            self.log_file.close()


    # connect logic
    
    def run(self, *args, **kwargs):
        token = None
        with open('oauth_token', 'r', encoding='UTF-8') as token_file:
            token = token_file.read()
        if token == None:
            self.logger.critical('Error reading OAuth Token')
            sys.exit(1)

        self.logger.info('Beginning connection.')

        try:
            self.loop.run_until_complete(self.start(token, reconnect=True))
        finally:
            self.loop.close()
