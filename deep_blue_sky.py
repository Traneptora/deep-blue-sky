#!/usr/bin/env python3

import asyncio, signal
import os, re, sys, time
import json
import traceback
import urllib.request
import contextlib, logging
import requests

import discord
from discord.ext import tasks

class DeepBlueSky(discord.Client):

    # command functions
    
    async def send_help(self, message: discord.Message, space_id, command_name, command_predicate):
        if self.is_moderator(message.author):
            help_lines = [f'`{command}`: {self.help_list[command]}' for command in self.help_list]
            help_string = '\n'.join(help_lines)
        else:
            help_string = 'Please send me a direct message.'
        await message.channel.send(help_string)

    async def change_prefix(self, message: discord.Message, space_id, command_name, command_predicate):
        if not self.is_moderator(message.author):
            await message.channel.send('Only moderators may do this.')
            return False
        if not command_predicate:
            await message.channel.send(f'New prefix may not be empty\nUsage: `{command_name} <new_prefix>`')
            return False
        new_prefix, split_predicate = self.split_command(command_predicate)
        if split_predicate:
            await message.channel.send(f'Invalid trailing predicate: `{split_predicate}`\nUsage: `{command_name} <new_prefix>`')
            return False
        if not new_prefix:
            await message.channel.send(f'New prefix may not be empty\nUsage: `{command_name} <new_prefix>`')
            return False
        if not re.match(r'[a-z0-9_\-!.\.?]+', new_prefix):
            await message.channel.send(f'Invalid prefix: {new_prefix}\nOnly ASCII alphanumeric characters or `-_!.?` permitted\nUsage: `{command_name} <new_prefix>`')
            return False
        if space_id not in self.space_overrides:
            self.space_overrides[space_id] = { 'id' : space_id }
        old_prefix = self.space_overrides[space_id].get('command_prefix', None)
        self.space_overrides[space_id]['command_prefix'] = new_prefix
        try:
            self.save_space_overrides(space_id)
        except IOError as error:
            if old_prefix: self.space_overrides[space_id]['command_prefix'] = old_prefix
            msg = 'Unknown error when changing prefix'
            self.logger.exception(msg)
            await message.channel.send(msg)
            return False
        await message.channel.send(f'Prefix for this space changed to `{new_prefix}`')
        return True

    async def reset_prefix(self, message: discord.Message, space_id, command_name, command_predicate):
        if not self.is_moderator(message.author):
            await message.channel.send('Only moderators may do this.')
            return False
        if command_predicate:
            await message.channel.send(f'Invalid trailing arguments: `{command_predicate}`\nUsage: `{command_name}`')
            return False
        if space_id not in self.space_overrides:
            self.space_overrides[space_id] = { 'id' : space_id }
        old_prefix = self.space_overrides[space_id].pop('command_prefix', None)
        try:
            self.save_space_overrides(space_id)
        except IOError as error:
            if old_prefix: self.space_overrides[space_id]['command_prefix'] = old_prefix
            msg = 'Unknown error when resetting prefix'
            self.logger.exception(msg)
            await message.channel.send(msg)
            return False
        await message.channel.send(f'Prefix for this space reset to the default, which is `{self.default_properties["command_prefix"]}`')
        return True

    async def new_command(self, message: discord.Message, space_id, command_name, command_predicate):
        if not command_predicate:
            await message.channel.send(f'Command name may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
            return False
        new_name, new_value = self.split_command(command_predicate)
        if not re.match(r'[a-z0-9_\-!\.?]+', new_name):
            await message.channel.send(f'Invalid command name: {new_name}\nOnly ASCII alphanumeric characters or `-_!.?` permitted\nUsage: `{command_name} <command_name> <command_value | attachment>`')
            return False
        if self.find_command(space_id, new_name, follow_alias=False):
            await message.channel.send(f'The command `{new_name}` already exists in this space. Try using `updatecommand` instead.')
            return False
        if not new_value: 
            if len(message.attachments) > 0:
                new_value = message.attachments[0].url
            else:
                await message.channel.send(f'Command value may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
                return False

        command = {
            'type' : 'simple',
            'author' : message.author.id,
            'value' : new_value
        }

        if space_id not in self.space_overrides:
            self.space_overrides[space_id] = { 'id' : space_id }

        if 'commands' in self.space_overrides[space_id]:
            self.space_overrides[space_id]['commands'][new_name] = command
        else:
            self.space_overrides[space_id]['commands'] = { new_name : command }

        try:
            self.save_command(space_id, new_name)
        except IOError as error:
            del self.space_overrides[space_id]['commands'][new_name]
            msg = 'Unknown error when registering command'
            self.logger.exception(msg)
            await message.channel.send(msg)
            return False
        await message.channel.send(f'Command added successfully. Try it with: `{self.get_in_space(space_id, "command_prefix")}{new_name}`')
        return True

    async def remove_command(self, message: discord.Message, space_id, command_name, command_predicate):
        if not command_predicate:
            await message.channel.send(f'Command name may not be empty\nUsage: `{command_name} <command_name>`')
            return False
        goodbye_name, split_predicate = self.split_command(command_predicate)
        if split_predicate:
            await message.channel.send(f'Invalid trailing arguments: `{split_predicate}`\nUsage: `{command_name} <command_name>`')
            return False
        if self.find_command('default', goodbye_name, follow_alias=False, use_default=False):
            await message.channel.send(f'Built-in commands cannot be removed.')
            return False
        command = self.find_command(space_id, goodbye_name, follow_alias=False)
        if not command:
            await message.channel.send(f'The command `{goodbye_name}` does not exist in this space.')
            return False
        if not self.is_moderator(message.author) and command['author'] != message.author.id:
            owner_user = await self.get_or_fetch_user(command['author'], channel=message.channel)
            if owner_user:
                await message.channel.send(f'The command `{goodbye_name}` belongs to `{str(owner_user)}`. You cannot remove it.')
                return False

        old_command = self.space_overrides[space_id]['commands'].pop(goodbye_name)
        try:
            self.save_command(space_id, goodbye_name)
        except IOError as error:
            self.space_overrides[space_id]['commands'][goodbye_name] = old_command
            msg = 'Unknown error when removing command'
            self.logger.exception(msg)
            await message.channel.send(msg)
            return False
        await message.channel.send(f'Command `{goodbye_name}` removed successfully.')
        return True

    async def update_command(self, message: discord.Message, space_id, command_name, command_predicate):
        if not command_predicate:
            await message.channel.send(f'Command name may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
            return False
        new_name, new_value = self.split_command(command_predicate)
        if not re.match(r'[a-z0-9_\-!\.?]+', new_name):
            await message.channel.send(f'Invalid command name: {new_name}\nOnly ASCII alphanumeric characters or `-_!.?` permitted\nUsage: `{command_name} <command_name> <command_value | attachment>`')
            return False
        if self.find_command('default', new_name, follow_alias=False, use_default=False):
            await message.channel.send(f'Built-in commands cannot be updated.')
            return False
        command = self.find_command(space_id, new_name, follow_alias=False)
        if not command:
            await message.channel.send(f'The command `{new_name}` does not exist in this space. Create it with `newcommand` instead.')
            return False
        if not self.is_moderator(message.author) and command['author'] != message.author.id:
            owner_user = await self.get_or_fetch_user(command['author'], channel=message.channel)
            if owner_user:
                await message.channel.send(f'The command `{new_name}` belongs to `{str(owner_user)}`. You cannot update it.')
                return False

        if not new_value:
            if len(message.attachments) > 0:
                new_value = message.attachments[0].url
            else:
                await message.channel.send(f'Command value may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
                return False

        old_value = command['value']
        command['value'] = new_value
        try:
            self.save_command(space_id, new_name)
        except IOError as error:
            command['value'] = old_value
            msg = 'Unknown error when updating command'
            self.logger.exception(msg)
            await message.channel.send(msg)
            return False
        await message.channel.send(f'Command updated successfully. Try it with: `{self.get_in_space(space_id, "command_prefix")}{new_name}`')
        return True

    async def list_commands(self, message: discord.Message, space_id, command_name, command_predicate):
        if command_predicate:
            await message.channel.send(f'Invalid trailing arguments: `{command_predicate}`\nUsage: `{command_name}`')
            return False
        if space_id not in self.space_overrides:
            self.space_overrides[space_id] = { 'id' : space_id }
        command_list = self.space_overrides[space_id].get('commands', {})
        owned_commands = []
        for custom_command in command_list:
            if command_list[custom_command]['author'] == message.author.id:
                owned_commands.append(custom_command)
        if len(owned_commands) == 0:
            await message.channel.send('You do not own any commands in this space.')
        else:
            await message.channel.send(f'You own the following commands:\n```{", ".join(owned_commands)}```')

    async def take_command(self, message: discord.Message, space_id, command_name, command_predicate):
        take_name, split_predicate = self.split_command(command_predicate)
        if split_predicate:
            await message.channel.send(f'Invalid trailing arguments: `{split_predicate}`\nUsage: `{command_name} <command_name>`')
            return False
        if self.find_command('default', take_name, follow_alias=False, use_default=False):
            await message.channel.send(f'Built-in commands cannot be taken.')
            return False
        command = self.find_command(space_id, take_name, follow_alias=False)
        if not command:
            await message.channel.send(f'That command does not exist in this space.')
            return False
        if not self.is_moderator(message.author) and command['author'] != message.author.id:
            owner_user = await self.get_or_fetch_user(command['author'], channel=message.channel)
            if owner_user:
                await message.channel.send(f'The command `{take_name}` belongs to `{str(owner_user)}`. You cannot take it.')
                return False
        old_author = command['author']
        command['author'] = message.author.id
        try:
            self.save_command(space_id, take_name)
        except IOError as error:
            command['author'] = old_author
            msg = 'Unknown error when taking command'
            self.logger.exception(msg)
            await message.channel.send(msg)
            return False
        await message.channel.send(f'Command ownership transfered successfully. You now own `{take_name}`.')
        return True

    def save_space_overrides(self, space_id):
        if space_id not in self.space_overrides:
            return False
        space = self.space_overrides[space_id]
        command_list = space.pop('commands', None)
        try:
            os.makedirs(f'storage/{space_id}/', mode=0o755, exist_ok=True)
            with open(f'storage/{space_id}/space.json', 'w', encoding='UTF-8') as json_file:
                json.dump(space, json_file)
        finally:
            if command_list: space['commands'] = command_list
        return True

    def save_command(self, space_id, command_name):
        if space_id not in self.space_overrides:
            return False
        space = self.space_overrides[space_id]
        if 'commands' not in space:
            return False
        os.makedirs(f'storage/{space_id}/commands/', mode=0o755, exist_ok=True)
        command_json_fname = f'storage/{space_id}/commands/{command_name}.json'
        if command_name in space['commands']:
            with open(command_json_fname, 'w', encoding='UTF-8') as json_file:
                json.dump(space['commands'][command_name], json_file)
        elif os.path.isfile(command_json_fname):
            os.remove(command_json_fname)

    def load_space_overrides(self):
        try:
            for space_id in os.listdir('storage/'):
                self.space_overrides[space_id] = {}
                space_json_fname = f'storage/{space_id}/space.json'
                if os.path.isfile(space_json_fname):
                    with open(space_json_fname, 'r', encoding='UTF-8') as json_file:
                        self.space_overrides[space_id] = json.load(json_file)
                if os.path.isdir(f'storage/{space_id}/commands/'):
                    self.space_overrides[space_id]['commands'] = {}
                    for command_json_fname in os.listdir(f'storage/{space_id}/commands/'):
                        with open(f'storage/{space_id}/commands/{command_json_fname}', encoding='UTF-8') as json_file:
                            self.space_overrides[space_id]['commands'][command_json_fname[:-5]] = json.load(json_file)
        except IOError as error:
            self.logger.exception('Unable to load space overrides')
            return False
        return True

    def get_space_id(self, message):
        if hasattr(message.channel, 'guild'):
            return f'guild_{message.channel.guild.id}'
        elif hasattr(message.channel, 'recipient'):
            return f'dm_{message.channel.recipient.id}'
        else:
            return f'chan_{message.channel.id}'

    def is_moderator(self, user):
        if hasattr(user, 'guild_permissions'):
            return user.guild_permissions.kick_members
        else:
            return True

    def get_in_space(self, space_id, key, use_default=True):
        if use_default:
            if space_id in self.space_overrides and key in self.space_overrides[space_id]:
                return self.space_overrides[space_id][key]
            else:
                return self.default_properties[key]
        else:
            if space_id in self.space_overrides:
                return self.space_overrides[space_id].get(key, None)
            else:
                return None

    def find_command(self, space_id, command_name, follow_alias=True, use_default=True):
        if command_name in self.builtin_commands:
            command = self.builtin_commands.get(command_name)
        else:
            command_list = self.get_in_space(space_id, 'commands', use_default=use_default)
            if command_list and command_name in command_list:
                command = command_list.get(command_name)
            else:
                return None
        if follow_alias and command['type'] == 'alias':
            return self.find_command(space_id, command['value'], follow_alias=False)
        else:
            return command

    async def get_or_fetch_user(self, user_id, channel=None):
        if hasattr(channel, 'guild'):
            return await self.get_or_fetch_member(channel.guild, user_id)
        user_obj = self.get_user(user_id)
        if user_obj:
            return user_obj
        try:
            user_obj = await self.fetch_user(user_id)
        except discord.HTTPException:
            return None
        return user_obj

    async def get_or_fetch_member(self, guild, user_id):
        member_obj = guild.get_member(user_id)
        if member_obj:
            return member_obj
        try:
            member_obj = await guild.fetch_member(user_id)
        except discord.HTTPException:
            return None
        return member_obj

    def split_command(self, command_string):
        args = command_string.split(maxsplit=1)
        if len(args) == 0:
            return (None, None)
        command_name = args[0]
        command_predicate = args[1] if len(args) > 1 else None
        command_name = command_name[0:64].rstrip(':').lower()
        return (command_name, command_predicate)

    async def process_command(self, message, space_id, command_string):
        command_name, command_predicate = self.split_command(command_string)
        if not command_name:
            return
        command = self.find_command(space_id, command_name)
        if command:
            if command['type'] == 'function':
                await command['value'](message, space_id, command_name, command_predicate)
            elif command['type'] in ('simple', 'alias'):
                await message.channel.send(command['value'])
            else:
                self.logger.error(f'Unknown command type: {command["type"]}')
        else:
            await message.channel.send(f'Unknown command in this space: `{command_name}`')


    # wikitext stuff
    
    async def set_wikitext(self, message, space_id, command_name, command_predicate):
        if not self.is_moderator(message.author):
            await message.channel.send('Only moderators may do this.')
            return False
        if not command_predicate:
            await message.channel.send(f'Choose enable or disable.\nUsage: `{command_name} <enable/disable>`')
            return False
        new_enabled, split_predicate = self.split_command(command_predicate)
        if split_predicate:
            await message.channel.send(f'Invalid trailing predicate: `{split_predicate}`\nUsage: `{command_name} <enable/disable>`')
            return False
        if not new_enabled:
            await message.channel.send(f'Choose enable or disable.\nUsage: `{command_name} <enable/disable>`')
            return False
        if re.match(r'yes|on|true|enabled?', new_enabled):
            new_value = True
        elif re.match(r'no|off|false|disabled?', new_enabled):
            new_value = False
        else:
            await message.channel.send(f'Invalid enable/disable value: `{new_enabled}`\nUsage: `{command_name} <enable/disable>`')
            return False
        if space_id not in self.space_overrides:
            self.space_overrides[space_id] = { 'id' : space_id }
        old_value = self.space_overrides[space_id].get('wikitext', None)
        self.space_overrides[space_id]['wikitext'] = new_value
        try:
            self.save_space_overrides(space_id)
        except IOError as error:
            if old_value: self.space_overrides[space_id]['wikitext'] = old_value
            msg = 'Unknown error when setting wikitext preferences'
            self.logger.exception(msg)
            await message.channel.send(msg)
            return False
        await message.channel.send(f'Wikitext for this space changed to `{new_value}`')
        return True

    async def reset_wikitext(self, message, space_id, command_name, command_predicate):
        if not self.is_moderator(message.author):
            await message.channel.send('Only moderators may do this.')
            return False
        if command_predicate:
            await message.channel.send(f'Invalid trailing predicate: `{command_predicate}`\nUsage: `{command_name}`')
            return False
        if space_id not in self.space_overrides:
            self.space_overrides[space_id] = { 'id' : space_id }
        old_value = self.space_overrides[space_id].pop('wikitext', None)
        try:
            self.save_space_overrides(space_id)
        except IOError as error:
            if old_value: self.space_overrides[space_id]['wikitext'] = old_value
            msg = 'Unknown error when resetting wikitext'
            self.logger.exception(msg)
            await message.channel.send(msg)
            return False
        await message.channel.send(f'Wikitext for this space reset to the default, which is `{self.default_properties["wikitext"]}`')
        return True


    def lookup_tvtropes(self, article):
        parts = re.sub(r'[^\w/]', '', article).split('/', maxsplit=1)
        if len(parts) > 1:
            namespace = parts[0]
            title = parts[1]
        else:
            namespace = 'Main'
            title = parts[0]
        server = 'https://tvtropes.org'
        query = '/pmwiki/pmwiki.php/' + namespace + '/' + title
        result = requests.get(server + query, allow_redirects=False)
        if 'location' in result.headers:
            location = re.sub(r'\?.*$', '', result.headers['location'])
            if location.startswith('/'):
                return (True, server + location)
            elif re.search(r'^[a-z]+://', location):
                return (True, location)
            else:
                return (True, server + '/pmwiki/pmwiki.php/' + namespace + '/' + location)
        result.encoding = 'UTF-8'
        if re.search(r"<div>Inexact title\. See the list below\. We don't have an article named <b>{}</b>/{}, exactly\. We do have:".format(namespace, title), result.text):
            return (False, result.url)
        else:
            if result.ok:
                return (True, result.url)
            else:
                return (False, None)

    def lookup_wikipedia(self, article):
        params = { 'title' : 'Special:Search', 'go' : 'Go', 'ns0' : '1', 'search' : article }
        result = requests.head('https://en.wikipedia.org/w/index.php', params=params)
        if 'location' in result.headers:
            return result.headers['location']
        else:
            return None

    def lookup_wikis(self, article):
        success, tv_url = self.lookup_tvtropes(article.strip())
        if success:
            return tv_url
        wiki_url = self.lookup_wikipedia(article)
        if wiki_url:
            return wiki_url
        if tv_url:
            return f'Inexact Title Disambiguation Page Found:\n{tv_url}'
        else:
            return f'Unable to locate article: `{article}`'

    async def handle_wiki_lookup(self, message):
        chunks = message.content.split('```')
        chunks = chunks[::2]
        articles = []
        for chunk in chunks:
            chunks_again = chunk.split('`')
            chunks_again = chunks_again[::2]
            for chunk_again in chunks_again:
                articles += re.findall(r'\[\[(.*?)\]\]', chunk_again)
        if (len(articles) > 0):
            await message.channel.send('\n'.join([self.lookup_wikis(article) for article in articles]))

    # events

    async def handle_message(self, message):
        if message.author == self.user:
            return
        if message.author.bot:
            return
        content = message.content.strip()
        space_id = self.get_space_id(message)
        command_prefix = self.get_in_space(space_id, 'command_prefix')
        if content.startswith(command_prefix):
            command_string = content[len(command_prefix):].strip()
            await self.process_command(message, space_id, command_string)
        elif self.get_in_space(space_id, 'wikitext'):
            await self.handle_wiki_lookup(message)

    # setup stuff

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('discord')
        self.logger.setLevel(logging.INFO)
        self.log_file = open('bot_output.log', mode='a', buffering=1, encoding='UTF-8')
        handler = logging.StreamHandler(stream=self.log_file)
        formatter = logging.Formatter(fmt='[{asctime}] {levelname}: {message}', style='{')
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
   
        super().__init__(*args, **kwargs)
        for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]:
            self.loop.add_signal_handler(sig, lambda sig = sig: asyncio.create_task(self.signal_handler(sig, self.loop)))

        self.builtin_commands = {
            'help' : {
                 'type' : 'function',
                 'author' : None,
                 'value' : self.send_help,
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
                'value' : self.change_prefix,
                'help' : 'Change the command prefix in this space'
            },
            'reset-prefix' : {
                'type' : 'function',
                'author' : None,
                'value' : self.reset_prefix,
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
                'value' : self.new_command,
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
                'value' : self.remove_command,
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
                'value' : self.update_command,
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
                'value' : self.list_commands,
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
            },
            'takecommand' : {
                'type' : 'function',
                'author' : None,
                'value' : self.take_command,
                'help' : 'Gain ownership of a simple command'
            },
            'wikitext' : {
                'type' : 'function',
                'author' : None,
                'value' : self.set_wikitext,
                'help' : 'Enable or disable wikitext in this space'
            },
            'setwikitext' : {
                'type' : 'alias',
                'author' : None,
                'value' : 'wikitext'
            },
            'reset-wikitext' : {
                'type' : 'function',
                'author' : None,
                'value' : self.reset_wikitext,
                'help' : 'Reset wikitext in this space to the default'
            },
            'resetwikitext' : {
                'type' : 'alias',
                'author' : None,
                'value' : 'reset-wikitext'
            }
        }

        self.help_list = {}
        for command in self.builtin_commands:
            if 'help' in self.builtin_commands[command]:
                self.help_list[command] = self.builtin_commands[command]['help']

        self.default_properties = {
            'id' : 'default',
            'command_prefix' : '--',
            'wikitext' : False,
            'commands' : {}
        }

        self.space_overrides = {}
        self.load_space_overrides()

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
