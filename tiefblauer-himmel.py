#!/usr/bin/env python3

import discord
from deep_blue_sky import DeepBlueSky

# Local deep blue sky logic

client = DeepBlueSky()

@client.event
async def on_ready():
    client.log_print(f'Logged in as {client.user}')
    game = discord.Game('-help')
    await client.change_presence(status=discord.Status.online, activity=game)

@client.event
async def on_message(message):
    if message.author == client.user:
        return
    if message.author.bot:
        return
    client.log_print(f'Received a message: {message.content}')

client.run()
