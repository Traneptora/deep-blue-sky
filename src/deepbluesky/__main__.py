#!/usr/bin/env python3

# pylint: disable=invalid-name

import asyncio
import discord
from .deepbluesky import DeepBlueSky

# Launch a default Deep Blue Sky bot

async def _main():
    client: DeepBlueSky = DeepBlueSky(bot_name='deep-blue-sky')
    @client.event
    async def on_message(message: discord.Message) -> None:
        await client.handle_message(message)
    async with client:
        await client.run_bot()

if __name__ == '__main__':
    asyncio.run(_main())
