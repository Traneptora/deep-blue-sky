#!/usr/bin/env python3

# pylint: disable=invalid-name

import discord
from deepbluesky import DeepBlueSky

# Launch a default Deep Blue Sky bot

client: DeepBlueSky = DeepBlueSky()

@client.event
async def on_message(message: discord.Message) -> None:
    await client.handle_message(message)

client.run()
