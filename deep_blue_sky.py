#!/usr/bin/env python3

import asyncio, signal
import os, re, sys, time
import traceback
import urllib.request
import contextlib

import discord
from discord.ext import tasks

class DeepBlueSky(discord.Client):

    # setup stuff

    def __init__(self, *args, **kwargs):
        self.log_file = open('bot_output.log', mode='a', buffering=1)
        with contextlib.redirect_stdout(self.log_file):
            with contextlib.redirect_stderr(self.log_file):
                discord.Client.__init__(self, *args, **kwargs)
        for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]:
            self.loop.add_signal_handler(sig, lambda sig = sig: asyncio.create_task(self.signal_handler(sig, self.loop)))

    def log_print(self, *args, **kwargs):
        print(time.strftime('[%Y-%m-%dT%H:%M:%S+00:00] ' , time.gmtime()), end='', file=self.log_file, flush=False)
        kwargs['file'] = self.log_file
        kwargs['flush'] = True
        kwargs['end'] = '\n'
        print(*args, **kwargs)

    def log_error(self, error: BaseException):
        self.log_print('Unexpected Exception')
        traceback.print_exc(file=self.log_file)


    # cleanup stuff

    async def cleanup(self):
        self.log_print('Received signal, exiting gracefully')
        await self.change_presence(status=discord.Status.invisible, activity=None)
        await self.close()
        self.log_print()

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
            self.log_print('Error reading OAuth Token')
            sys.exit(1)

        self.log_print('Beginning connection.')

        try:
            self.loop.run_until_complete(self.start(token, reconnect=True))
        finally:
            self.loop.close()
