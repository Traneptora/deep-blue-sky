#!/usr/bin/env python3

# pylint: disable=invalid-name

import discord
from .deepbluesky import DeepBlueSky

# Launch a default Deep Blue Sky bot

def _main():
    client: DeepBlueSky = DeepBlueSky(bot_name='deep-blue-sky')
    @client.event
    async def on_message(message: discord.Message) -> None:
        await client.handle_message(message)
    client.run()

if __name__ == '__main__':
    _main()
