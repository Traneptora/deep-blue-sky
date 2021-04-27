#!/usr/bin/env python3

import discord
from deep_blue_sky import DeepBlueSky

# Local deep blue sky logic

client = DeepBlueSky()

async def send_help(message: discord.Message, space_id):
    await message.channel.send('Help is on the way!')

builtin_commands = {
    'help' : {
         'type' : 'function',
         'author' : None,
         'value' : send_help,
    },
    'halp' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'help'
    },
    'ping' : {
        'type' : 'simple',
        'author' : None,
        'value' : 'pong'
    }
}

default_properties = {
    'id' : 'default',
    'command_prefix' : '-',
    'callme' : None,
    'commands' : {}
}

space_overrides = {
    
}

@client.event
async def on_ready():
    client.log_print(f'Logged in as {client.user}')
    game = discord.Game('-help')
    await client.change_presence(status=discord.Status.online, activity=game)

def get_space_id(message):
    if message.channel is discord.TextChannel:
        return f'guild_{message.channel.guild.id}'
    if message.channel is discord.DMChannel:
        return f'dm_{message.channel.recipient.id}'
    if message.channel is discord.GroupChannel:
        return f'group_{message.channel.id}'
    return None

def get_in_space(space_id, key):
    space_properties = space_overrides.get(space_id, default_properties)
    return space_properties.get(key, default_properties.get(key))

def find_command(space_id, command_name, follow_alias=True):
    if command_name in builtin_commands:
        command = builtin_commands.get(command_name)
    else:
        command_list = get_in_space(space_id, 'commands')
        if command_name in command_list:
            command = command_list.get(command_name)
        else:
            return None
    if follow_alias and command['type'] == 'alias':
        return find_command(space_id, command['value'], follow_alias=False)
    else:
        return command

async def process_command(message, space_id, command_string):
    args = command_string.split()
    if len(args) == 0:
        return
    command_name, *arglist = args
    command_name = command_name[0:64].lower()
    command = find_command(space_id, command_name)
    if command:
        if command['type'] == 'function':
            await command['value'](message, space_id)
        elif command['type'] in ('simple', 'alias'):
            await message.channel.send(command['value'])
        else:
            client.log_print(f'Unknown command type: {command["type"]}')
    else:
        await message.channel.send(f'Unknown command in this space: `{command_name}`')

@client.event
async def on_message(message):
    if message.author == client.user:
        return
    if message.author.bot:
        return
    content = message.content.strip()
    space_id = get_space_id(message)
    command_prefix = get_in_space(space_id, 'command_prefix')
    if content.startswith(command_prefix):
        command_string = content.removeprefix(command_prefix)
        await process_command(message, space_id, command_string)

client.run()
