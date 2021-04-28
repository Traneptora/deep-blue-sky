#!/usr/bin/env python3

import os, re, json

import discord
from deep_blue_sky import DeepBlueSky

# Local deep blue sky logic

client = DeepBlueSky()

async def send_help(message: discord.Message, space_id, command_name, *args):
    if is_moderator(message.author):
        help_lines = [f'`{command}`: {help_list[command]}' for command in help_list]
        help_string = '\n'.join(help_lines)
    else:
        help_string = 'Please send me a direct message.'
    await message.channel.send(help_string)

async def change_prefix(message: discord.Message, space_id, command_name, *args):
    if not is_moderator(message.author):
        await message.channel.send('Only moderators may do this.')
        return False
    if len(args) > 1:
        await message.channel.send(f'Invalid trailing arguments: `{"`, `".join(args[1:])}`\nUsage: `{command_name} <new_prefix>`')
        return False
    if len(args) == 0:
        await message.channel.send(f'New prefix may not be empty\nUsage: `{command_name} <new_prefix>`')
        return False
    new_prefix = args[0].lower()
    if not re.match(r'[a-z0-9_\-!.\.?]+', new_prefix):
        await message.channel.send(f'Invalid prefix: {new_prefix}\nOnly ASCII alphanumeric characters or `-_!.?` permitted\nUsage: `{command_name} <new_prefix>`')
        return False
    if space_id not in space_overrides:
        space_overrides[space_id] = { 'id' : space_id }
    old_prefix = space_overrides[space_id].get('command_prefix', None)
    space_overrides[space_id]['command_prefix'] = new_prefix
    try:
        save_space_overrides(space_id)
    except IOError as error:
        if old_prefix: space_overrides[space_id]['command_prefix'] = old_prefix
        client.log_error(error)
        await message.channel.send('Unknown error when changing prefix')
        return False
    await message.channel.send(f'Prefix for this space changed to `{new_prefix}`')
    return True

async def reset_prefix(message: discord.Message, space_id, command_name, *args):
    if not is_moderator(message.author):
        await message.channel.send('Only moderators may do this.')
        return False
    if len(args) > 0:
        await message.channel.send(f'Invalid trailing arguments: `{"`, `".join(args)}`\nUsage: `{command_name}`')
        return False
    if space_id not in space_overrides:
        space_overrides[space_id] = { 'id' : space_id }
    old_prefix = space_overrides[space_id].pop('command_prefix', None)
    try:
        save_space_overrides(space_id)
    except IOError as error:
        if old_prefix: space_overrides[space_id]['command_prefix'] = old_prefix
        client.log_error(error)
        await message.channel.send('Unknown error when resetting prefix')
        return False
    await message.channel.send(f'Prefix for this space reset to the default, which is `{default_properties["command_prefix"]}`')
    return True

async def new_command(message: discord.Message, space_id, command_name, *args):
    if len(args) == 0:
        await message.channel.send(f'Command name may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
        return False
    new_name = args[0]
    if not re.match(r'[a-z0-9_\-!\.?]+', new_name):
        await message.channel.send(f'Invalid command name: {new_name}\nOnly ASCII alphanumeric characters or `-_!.?` permitted\nUsage: `{command_name} <command_name> <command_value | attachment>`')
        return False
    if find_command(space_id, new_name, follow_alias=False):
        await message.channel.send(f'That command already exists in this space. Try using `updatecommand` instead.')
        return False
    if len(args) == 1: 
        if len(message.attachments) > 0:
            new_value = message.attachments[0].url
        else:
            await message.channel.send(f'Command value may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
            return False
    else:
        new_value = ' '.join(args[1:])

    command = {
        'type' : 'simple',
        'author' : message.author.id,
        'value' : new_value
    }

    if space_id not in space_overrides:
        space_overrides[space_id] = { 'id' : space_id }

    if 'commands' in space_overrides[space_id]:
        space_overrides[space_id]['commands'][new_name] = command
    else:
        space_overrides[space_id]['commands'] = { new_name : command }

    try:
        save_command(space_id, new_name)
    except IOError as error:
        del space_overrides[space_id]['commands'][new_name]
        client.log_error(error)
        await message.channel.send('Unknown error when registering command')
        return False
    await message.channel.send(f'Command added successfully. Try it with: `{get_in_space(space_id, "command_prefix")}{new_name}`')
    return True

async def remove_command(message: discord.Message, space_id, command_name, *args):
    if len(args) == 0:
        await message.channel.send(f'Command name may not be empty\nUsage: `{command_name} <command_name>`')
        return False
    if len(args) > 1:
        await message.channel.send(f'Invalid trailing arguments: `{"`, `".join(args[1:])}`\nUsage: `{command_name} <command_name>>`')
        return False
    goodbye_name = args[0]
    if find_command('default', goodbye_name, follow_alias=False, use_default=False):
        await message.channel.send(f'Built-in commands cannot be removed.')
        return False
    command = find_command(space_id, goodbye_name, follow_alias=False)
    if not command:
        await message.channel.send(f'That command does not exist in this space.')
        return False
    if not is_moderator(message.author) and command['author'] != message.author.id:
        await message.channel.send(f'That command does not belong to you. It belongs to `{client.get_user(command["author"])}`')
        return False
    
    old_command = space_overrides[space_id]['commands'].pop(goodbye_name)
    try:
        save_command(space_id, goodbye_name)
    except IOError as error:
        space_overrides[space_id]['commands'][goodbye_name] = old_command
        client.log_error(error)
        await message.channel.send('Unknown error when removing command')
        return False
    await message.channel.send(f'Command removed successfully.')
    return True

async def update_command(message: discord.Message, space_id, command_name, *args):
    if len(args) == 0:
        await message.channel.send(f'Command name may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
        return False
    new_name = args[0]
    if not re.match(r'[a-z0-9_\-!\.?]+', new_name):
        await message.channel.send(f'Invalid command name: {new_name}\nOnly ASCII alphanumeric characters or `-_!.?` permitted\nUsage: `{command_name} <command_name> <command_value | attachment>`')
        return False
    if find_command('default', new_name, follow_alias=False, use_default=False):
        await message.channel.send(f'Built-in commands cannot be updated.')
        return False
    command = find_command(space_id, new_name, follow_alias=False)
    if not command:
        await message.channel.send(f'That command does not exist in this space. Create it with `newcommand` instead.')
        return False
    if not is_moderator(message.author) and command['author'] != message.author.id:
        await message.channel.send(f'That command does not belong to you. It belongs to `{client.get_user(command["author"])}`')
        return False
    if len(args) == 1: 
        if len(message.attachments) > 0:
            new_value = message.attachments[0].url
        else:
            await message.channel.send(f'Command value may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
            return False
    else:
        new_value = ' '.join(args[1:])

    old_value = command['value']
    command['value'] = new_value
    try:
        save_command(space_id, new_name)
    except IOError as error:
        command['value'] = old_value
        client.log_error(error)
        await message.channel.send('Unknown error when updating command')
        return False
    await message.channel.send(f'Command updated successfully. Try it with: `{get_in_space(space_id, "command_prefix")}{new_name}`')
    return True

async def list_commands(message: discord.Message, space_id, command_name, *args):
    if len(args) > 0:
        await message.channel.send(f'Invalid trailing arguments: `{"`, `".join(args)}`\nUsage: `{command_name}`')
        return False
    if space_id not in space_overrides:
        space_overrides[space_id] = { 'id' : space_id }
    command_list = space_overrides[space_id].get('commands', {})
    owned_commands = []
    for custom_command in command_list:
        if command_list[custom_command]['author'] == message.author.id:
            owned_commands.append(custom_command)
    if len(owned_commands) == 0:
        await message.channel.send('You do not own any commands in this space.')
    else:
        await message.channel.send(f'You own the following commands:\n```{", ".join(owned_commands)}```')

def save_space_overrides(space_id):
    if space_id not in space_overrides:
        return False
    space = space_overrides[space_id]
    command_list = space.pop('commands', None)
    try:
        os.makedirs(f'storage/{space_id}/', mode=0o755, exist_ok=True)
        with open(f'storage/{space_id}/space.json', 'w') as json_file:
            json.dump(space, json_file)
    finally:
        if command_list: space['commands'] = command_list
    return True

def save_command(space_id, command_name):
    if space_id not in space_overrides:
        return False
    space = space_overrides[space_id]
    if 'commands' not in space:
        return False
    os.makedirs(f'storage/{space_id}/commands/', mode=0o755, exist_ok=True)
    command_json_fname = f'storage/{space_id}/commands/{command_name}.json'
    if command_name in space['commands']:
        with open(command_json_fname, 'w') as json_file:
            json.dump(space['commands'][command_name], json_file)
    elif os.path.isfile(command_json_fname):
        os.remove(command_json_fname)

builtin_commands = {
    'help' : {
         'type' : 'function',
         'author' : None,
         'value' : send_help,
         'help' : 'Print help messages.'
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
    },
    'change-prefix' : {
        'type' : 'function',
        'author' : None,
        'value' : change_prefix,
        'help' : 'Change the command prefix in this space'
    },
    'reset-prefix' : {
        'type' : 'function',
        'author' : None,
        'value' : reset_prefix,
        'help' : 'Restore the command prefix in this space to the default value'
    },
    'changeprefix' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'change-prefix'
    },
    'resetprefix' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'reset-prefix'
    },
    'newcommand' : {
        'type' : 'function',
        'author' : None,
        'value' : new_command,
        'help' : 'Create a new simple command'
    },
    'addcommand' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'newcommand'
    },
    'createcommand' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'newcommand'
    },
    'addc' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'newcommand'
    },
    'removecommand' : {
        'type' : 'function',
        'author' : None,
        'value' : remove_command,
        'help' : 'Remove a simple command with a specific name'
    },
    'deletecommand' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'removecommand'
    },
    'delc' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'removecommand'
    },
    'updatecommand' : {
        'type' : 'function',
        'author' : None,
        'value' : update_command,
        'help' : 'Change the value of a simple command'
    },
    'renewcommand' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'updatecommand'
    },
    'fixc' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'updatecommand'
    },
    'listcommands' : {
        'type' : 'function',
        'author' : None,
        'value' : list_commands,
        'help' : 'List simple commands you own'
    },
    'commandlist' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'listcommands'
    },
    'clist' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'listcommands'
    },
    'listc' : {
        'type' : 'alias',
        'author' : None,
        'value' : 'listcommands'
    }
}

help_list = {}
for command in builtin_commands:
    if 'help' in builtin_commands[command]:
        help_list[command] = builtin_commands[command]['help']

default_properties = {
    'id' : 'default',
    'command_prefix' : '--',
    'callme' : None,
    'commands' : {}
}

space_overrides = {
    
}

def load_space_overrides():
    try:
        for space_id in os.listdir('storage/'):
            with open(f'storage/{space_id}/space.json', 'r') as json_file:
                space_overrides[space_id] = json.load(json_file)
            if os.path.isdir(f'storage/{space_id}/commands/'):
                space_overrides[space_id]['commands'] = {}
                for command_json_fname in os.listdir(f'storage/{space_id}/commands/'):
                    with open(f'storage/{space_id}/commands/{command_json_fname}') as json_file:
                        space_overrides[space_id]['commands'][command_json_fname.removesuffix('.json')] = json.load(json_file)
    except IOError as error:
        client.log_print('Unable to load space overrides')
        client.log_error(error)
        return False
    return True

@client.event
async def on_ready():
    client.log_print(f'Logged in as {client.user}')
    game = discord.Game('--help')
    await client.change_presence(status=discord.Status.online, activity=game)
    load_space_overrides()

def get_space_id(message):
    if hasattr(message.channel, 'guild'):
        return f'guild_{message.channel.guild.id}'
    elif hasattr(message.channel, 'recipient'):
        return f'dm_{message.channel.recipient.id}'
    else:
        return f'chan_{message.channel.id}'

def is_moderator(user):
    if hasattr(user, 'guild_permissions'):
        return user.guild_permissions.kick_members
    else:
        return True

def get_in_space(space_id, key, use_default=True):
    if use_default:
        if space_id in space_overrides and key in space_overrides[space_id]:
            return space_overrides[space_id][key]
        else:
            return default_properties[key]
    else:
        if space_id in space_overrides:
            return space_overrides[space_id].get(key, None)
        else:
            return None

def find_command(space_id, command_name, follow_alias=True, use_default=True):
    if command_name in builtin_commands:
        command = builtin_commands.get(command_name)
    else:
        command_list = get_in_space(space_id, 'commands', use_default=use_default)
        if command_list and command_name in command_list:
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
    command_name = command_name[0:64].rstrip(':').lower()
    command = find_command(space_id, command_name)
    if command:
        if command['type'] == 'function':
            await command['value'](message, space_id, command_name, *arglist)
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
        command_string = content.removeprefix(command_prefix).strip()
        await process_command(message, space_id, command_string)

client.run()
