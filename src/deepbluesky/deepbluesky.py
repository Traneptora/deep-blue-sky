# deepbluesky.py

# Deep Blue Sky python discord bot
# This file doesn't actually create a bot
# For the reference implementation, see tiefblauer-himmel.py

import abc
import asyncio
import functools
import json
import logging
import os
import re
import signal
import sys
import time

from collections import OrderedDict
from typing import Any, Callable, Literal, Optional, Union
from typing import Dict, FrozenSet, Iterable, List, Tuple

import discord
import requests

from .space import Space
from .space import ChannelSpace, DMSpace, GuildSpace
from .command import Command
from .command import CommandAlias, CommandFunction, CommandSimple

def identity(arg: Any) -> Any:
    return arg

def owoify(text: str) -> str:
    text = re.sub(r'r{1,2}|l{1,2}', 'w', text)
    text = re.sub(r'R{1,2}|L{1,2}', 'W', text)
    text = re.sub(r'([Nn])(?=[AEIOUYaeiouy])', r'\1y', text)
    return text

def spongebob(text: str) -> str:
    total = ''
    upper = False
    for char in text.lower():
        # space characters and the like are not
        # lowercase even if the string is lowercase
        if char.islower():
            if upper:
                total += char.upper()
            else:
                total += char
            upper = not upper
        else:
            total += char
    return total

def removeprefix(base: str, prefix: str) -> str:
    try:
        return base.removeprefix(prefix)
    except AttributeError:
        pass
    if base.startswith(prefix):
        return base[len(prefix):]
    return base

def removesuffix(base: str, suffix: str) -> str:
    try:
        return base.removesuffix(suffix)
    except AttributeError:
        pass
    if base.endswith(suffix):
        return base[:len(suffix)]
    return base

def relative_to_absolute_location(location: str, query_url: str) -> str:
    query_url = re.sub(r'\?.*$', '', query_url)
    if location.startswith('/'):
        server = re.sub(r'^([a-zA-Z]+://[^/]*)/.*$', r'\1', query_url)
        return server + location
    if re.match(r'^[a-zA-Z]+://', location):
        return location
    return re.sub(r'^(([^/]*/)+)[^/]*', r'\1', query_url) + '/' + location

def lookup_tvtropes(article: str) -> Tuple[bool, str]:
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
        location = relative_to_absolute_location(result.headers['location'], server + query)
        return (True, location)
    result.encoding = 'UTF-8'
    if re.search(r"<div>Inexact title\. See the list below\. We don't have an article named <b>{}</b>/{}, exactly\. We do have:".format(namespace, title), result.text, flags=re.IGNORECASE):
        return (False, result.url)
    return (True, result.url) if result.ok else (False, '')

def lookup_mediawiki(mediawiki_base: str, article: str) -> Optional[str]:
    parts = article.split('/')
    parts = [re.sub(r'^\s*([^\s]+(\s+[^\s]+)*)\s*$', r'\1', part) for part in parts]
    parts = [re.sub(r'\s', r'_', part) for part in parts]
    article = '/'.join(parts)
    params = { 'title' : 'Special:Search', 'go' : 'Go', 'ns0' : '1', 'search' : article }
    result = requests.head(mediawiki_base, params=params)
    if 'location' in result.headers:
        location = relative_to_absolute_location(result.headers['location'], mediawiki_base)
        if ':' in location[7:]:
            second_result = requests.head(location)
            return location if second_result.ok and 'last-modified' in second_result.headers else None
        return location
    return None

def lookup_wikis(article: str, extra_wikis: List[str]) -> str:
    for wiki in extra_wikis:
        wiki_url = lookup_mediawiki(wiki, article)
        if wiki_url:
            return wiki_url
    success, tv_url = lookup_tvtropes(article.strip())
    if success:
        return tv_url
    wiki_url = lookup_mediawiki('https://en.wikipedia.org/w/index.php', article)
    if wiki_url:
        return wiki_url
    return f'Inexact Title Disambiguation Page Found:\n{tv_url}' if tv_url else f'Unable to locate article: `{article}`'

def snowflake_list(snowflake_input: Optional[Union[str, discord.abc.Snowflake, int, List[Union[str, discord.abc.Snowflake, int]]]]) -> List[int]:
    if not snowflake_input:
        return []

    try:
        snowflake_input = int(snowflake_input) # type: ignore
    # intentionally not catching ValueError here, since string IDs should cast to int
    # TypeError means not a snowflake, string, or int, so probably an iterable
    except TypeError:
        pass

    try:
        snowflake_input = list(snowflake_input) # type: ignore
    # probably an integer
    except TypeError:
        snowflake_input = [snowflake_input]

    return [int(snowflake) for snowflake in snowflake_input] # type: ignore

def split_command(command_string: Optional[str]) -> Tuple[str, Optional[str]]:
    if not command_string:
        return ('', None)
    sname: Optional[str]
    predicate: Optional[str]
    sname, predicate, *_ = *command_string.split(maxsplit=1), None, None
    name: str = sname[:64].rstrip(':,').lower() if sname else ''
    return (name, predicate)

def chunk_message(message_string: str, chunk_delimiter: str) -> Tuple[List[str], List[str]]:
    chunks = message_string.split(chunk_delimiter)
    if len(chunks) % 2 == 0:
        noncode_chunks = chunks[::2] + chunks[-1:]
        code_chunks = chunks[1:-1:2]
    else:
        noncode_chunks = chunks[::2]
        code_chunks = chunks[1::2]
    return (noncode_chunks, code_chunks)

def assemble_message(noncode_chunks: List[str], code_chunks: List[str], chunk_delimiter: str) -> str:
    if (len(noncode_chunks) + len(code_chunks)) % 2 == 0:
        chunk_interleave = [val for pair in zip(noncode_chunks[:-2], code_chunks) for val in pair] + noncode_chunks[-2:]
    else:
        chunk_interleave = [val for pair in zip(noncode_chunks[:-1], code_chunks) for val in pair] + noncode_chunks[-1:]
    return chunk_delimiter.join(chunk_interleave)

def get_all_noncode_chunks(message_string: str) -> List[str]:
    chunks: Iterable[str]
    chunks, _ = chunk_message(message_string, '```')
    chunks, _ = zip(*[chunk_message(chunk, '`') for chunk in chunks])
    # zip will zip this into a tuple
    # cast to list to return a proper list
    return [y for x in chunks for y in x]

class DeepBlueSky(discord.Client):

    async def send_to_channel(self, channel: discord.abc.Messageable, message_to_send: str, ping_user=None, ping_roles=None):
        ping_user = [self.get_or_fetch_user(user) for user in snowflake_list(ping_user)]
        if hasattr(channel, 'guild'):
            ping_roles = [channel.guild.get_role(role) for role in snowflake_list(ping_roles)]
        else:
            ping_roles = []
        await channel.send(message_to_send, allowed_mentions=discord.AllowedMentions(users=ping_user, roles=ping_roles))

    # command functions

    async def send_help(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        wanted_help: str
        wanted_help, _ = split_command(command_predicate)
        if not wanted_help:
            if space.is_moderator(trigger.author):
                help_string = '\n'.join([f'`{command.name}`: {command.get_help()}' for command in self.builtin_command_dict.values() if command.command_type != 'alias'])
                success = True
            else:
                help_string = 'Please send me a direct message (this is spammy)'
                success = False
        else:
            command = self.find_command(space, wanted_help, follow_alias=False)
            if command:
                help_string = command.get_help()
                success = True
            else:
                help_string = f'Cannot provide help in this space for: `{wanted_help}`'
                success = False
        await self.send_to_channel(trigger.channel, help_string)
        return success

    async def change_prefix(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        if not space.is_moderator(trigger.author):
            await self.send_to_channel(trigger.channel, 'Only moderators may do this.')
            return False
        usage = f'Usage: `{command_name}` <new_prefix>'
        value: str
        value, _ = split_command(command_predicate)
        if not value:
            await self.send_to_channel(trigger.channel, f'New prefix may not be empty\n{usage}')
            return False
        if not re.match(r'^[a-z0-9_\-!\.?]+$', value):
            await self.send_to_channel(trigger.channel, f'Invalid prefix: `{value}`\nOnly ASCII alphanumeric characters or `-_!.?` permitted\n{usage}')
            return False
        space.command_prefix = value
        success = space.save()
        msg = f'Prefix for this space changed to `{value}`' if success else 'Unknown error when saving properties'
        await self.send_to_channel(trigger.channel, msg)
        return success

    async def reset_prefix(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        if not space.is_moderator(trigger.author):
            await self.send_to_channel(trigger.channel, 'Only moderators may do this.')
            return False
        space.command_prefix = None
        success = space.save()
        msg = f'Prefix for this space reset to the default, which is `{self.default_properties["command_prefix"]}`' if success else 'Unknown error when saving properties'
        await self.send_to_channel(trigger.channel, msg)
        return success

    async def create_command(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        usage = f'Usage: `{command_name}` <command_name> <command_value | attachment>'
        new_name, new_value = split_command(command_predicate)
        if not new_name:
            await self.send_to_channel(trigger.channel, f'Command name may not be empty\n{usage}')
            return False
        if not re.match(r'^[a-z0-9_\-!\.?]+$', new_name):
            await self.send_to_channel(trigger.channel, f'Invalid command name: `{new_name}`\nOnly ASCII alphanumeric characters or `-_!.?` permitted\n{usage}')
            return False
        if self.find_command(space, new_name, follow_alias=False):
            await self.send_to_channel(trigger.channel, f'The command `{new_name}` already exists in this space. Use `updatecommand` instead.')
            return False
        lines = [x.strip() for x in [new_value] if x] + [attachment.url for attachment in trigger.attachments]
        if len(lines) == 0:
            await self.send_to_channel(trigger.channel, f'Command value may not be empty\n{usage}')
            return False
        new_value = '\n'.join(lines)
        command = CommandSimple(name=new_name, value=new_value, author=trigger.author.id, creation_time=int(time.time()), modification_time=int(time.time()))
        space.custom_command_dict[new_name] = command
        success = space.save_command(new_name)
        msg = f'Command added successfully. Try it with: `{self.get_property(space, "command_prefix")}{new_name}`' if success else 'Unknown error when evaluating command'
        await self.send_to_channel(trigger.channel, msg)
        return success

    async def user_exists(self, user_id: int, channel: discord.abc.Messageable) -> bool:
        user = await self.get_or_fetch_user(user_id, channel=channel)
        return user is not None

    async def remove_command(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        usage = f'Usage: `{command_name}` <command_names...>'
        new_name, remainder = split_command(command_predicate)
        if not new_name:
            await self.send_to_channel(trigger.channel, f'Command name may not be empty\n{usage}')
            return False
        # This is a security feature in case there's a bug
        if remainder and not space.is_moderator(trigger.author):
            await self.send_to_channel(trigger.channel, 'Only moderators may remove commands in bulk.')
            return False
        command_set = {new_name}
        while remainder:
            new_name, remainder = split_command(remainder)
            command_set.add(new_name)
        for name in command_set:
            if name in self.builtin_command_dict:
                await self.send_to_channel(trigger.channel, 'Built-in commands cannot be removed.')
                return False
            if name not in space.custom_command_dict:
                await self.send_to_channel(trigger.channel, f'Unknown command in this space: `{name}`')
                return False
            author_id = space.custom_command_dict[name].author
            if author_id and author_id != trigger.author.id and not space.is_moderator(trigger.author) and await self.user_exists(author_id, trigger.channel):
                await self.send_to_channel(trigger.channel, f'The command `{name}` blongs to <@!{author_id}>. You cannot remove it.')
                return False
        success = True
        success_list = []
        while len(command_set) > 0:
            for name in list(command_set):
                command = space.custom_command_dict[name]
                command_set.update({alias.name for alias in command.aliases})
                if isinstance(command, CommandAlias):
                    command.follow().aliases.remove(command)
                del space.custom_command_dict[name]
                if space.save_command(name):
                    success_list += [name]
                else:
                    success=False
                command_set.remove(name)
        msg = f'Command removed successfully: `{", ".join(success_list)}`' if success else 'Unknown error when evaluating command'
        await self.send_to_channel(trigger.channel, msg)
        return success

    async def update_command(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        usage = f'Usage: `{command_name}` <command_name> <command_value | attachment>'
        new_name, new_value = split_command(command_predicate)
        if not new_name:
            await self.send_to_channel(trigger.channel, f'Command name may not be empty\n{usage}')
            return False
        if not re.match(r'^[a-z0-9_\-!\.?]+$', new_name):
            await self.send_to_channel(trigger.channel, f'Invalid command name: {new_name}\nOnly ASCII alphanumeric characters or `-_!.?` permitted\n{usage}')
            return False
        if new_name in self.builtin_command_dict:
            await self.send_to_channel(trigger.channel, 'Built-in commands cannot be updated.')
            return False
        if new_name not in space.custom_command_dict:
            await self.send_to_channel(trigger.channel, f'Unknown command in this space: `{new_name}`')
            return False
        command = space.custom_command_dict[new_name]
        if command.author and command.author != trigger.author.id and not space.is_moderator(trigger.author) and await self.user_exists(command.author, trigger.channel):
            await self.send_to_channel(trigger.channel, f'The command `{command.name}` blongs to <@!{command.author}>. You cannot update it.')
            return False
        lines = [x.strip() for x in [new_value] if x] + [attachment.url for attachment in trigger.attachments]
        if len(lines) == 0:
            await self.send_to_channel(trigger.channel, f'Command value may not be empty\n{usage}')
            return False
        new_value = '\n'.join(lines)
        if isinstance(command, CommandSimple):
            command.value = new_value
        else:
            self.logger.critical(f'custom command not simple: {command}')
            await self.send_to_channel(trigger.channel, 'Unknown error when evaluating command')
            return False
        success = space.save_command(new_name)
        msg = f'Command updated successfully. Try it with: `{self.get_property(space, "command_prefix")}{new_name}`' if success else 'Unknown error when evaluating command'
        await self.send_to_channel(trigger.channel, msg)
        return success

    async def list_commands(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        user_id = await space.query_users(command_predicate) if command_predicate else trigger.author.id
        if user_id == -2:
            await self.send_to_channel(trigger.channel, f'More than one user matched query: `{command_predicate}`')
            return False
        if user_id == -1:
            await self.send_to_channel(trigger.channel, f'Could not find user: `{command_predicate}`')
            return False
        owned_commands = [command.name for command in space.custom_command_dict.values() if command.author == user_id]
        msg = (f'No owned commands in this space for <@!{user_id}>'
            if len(owned_commands) == 0
            else f'<@!{user_id}> owns the following commands in this space:\n```{", ".join(owned_commands)}```')
        await self.send_to_channel(trigger.channel, msg)
        return True

    async def _give_command0(self, trigger: discord.Message, space: Space, command_name: str, remainder: Optional[str], verb: str, participle: str, usage: str, give_id: int) -> bool:
        new_name, remainder = split_command(remainder)
        if not new_name:
            await self.send_to_channel(trigger.channel, f'Command name may not be empty\n{usage}')
            return False
        # This is a security feature in case there's a bug
        if remainder and not space.is_moderator(trigger.author):
            await self.send_to_channel(trigger.channel, f'Only moderators may {verb} commands in bulk.')
            return False
        command_set = {new_name}
        while remainder:
            new_name, remainder = split_command(remainder)
            command_set.add(new_name)
        for name in command_set:
            if name in self.builtin_command_dict:
                await self.send_to_channel(trigger.channel, f'Built-in commands cannot be {participle}.')
                return False
            if name not in space.custom_command_dict:
                await self.send_to_channel(trigger.channel, f'Unknown command in this space: `{name}`')
                return False
            author_id = space.custom_command_dict[name].author
            if author_id and author_id != trigger.author.id and not space.is_moderator(trigger.author) and await self.user_exists(author_id, trigger.channel):
                await self.send_to_channel(trigger.channel, f'The command `{name}` blongs to <@!{author_id}>. You cannot {verb} it.')
                return False
        success = True
        success_list = []
        for name in command_set:
            space.custom_command_dict[name].author = give_id
            if space.save_command(name):
                success_list += [name]
            else:
                success = False
        msg = f'Command ownership transfered successfully for: `{", ".join(success_list)}`' if success else 'Unknown error when evaluating command'
        await self.send_to_channel(trigger.channel, msg)
        return success

    async def take_command(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        usage = f'Usage: `{command_name} <command_names...>`'
        return await self._give_command0(trigger, space, command_name, remainder=command_predicate, verb='take', participle='taken', usage=usage, give_id=trigger.author.id)

    async def give_command(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        usage = f'Usage: `{command_name} <user_spec> <command_names...>`'
        if not command_predicate:
            await self.send_to_channel(trigger.channel, f'Provide user and command name\n{usage}')
            return False
        match = re.match(r'[^#]+#[0-9]{4}(?=\s+)', command_predicate)
        user_str: str
        remainder: Optional[str]
        if match:
            user_str = match.group()
            remainder = command_predicate[match.end():]
        else:
            user_str, remainder = split_command(command_predicate)
        user_id = await space.query_users(user_str)
        if user_id == -2:
            await self.send_to_channel(trigger.channel, f'More than one user matched query: `{user_str}`')
            return False
        if user_id == -1:
            await self.send_to_channel(trigger.channel, f'Could not find user: `{user_str}`')
            return False
        return await self._give_command0(trigger, space, command_name, remainder=remainder, verb='give', participle='given', usage=usage, give_id=user_id)

    async def who_owns_command(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        usage = f'Usage: `{command_name}` <command_name>'
        name, _ = split_command(command_predicate)
        if not name:
            await self.send_to_channel(trigger.channel, f'Command name may not be empty\n{usage}')
            return False
        if name in self.builtin_command_dict:
            await self.send_to_channel(trigger.channel, f'The command `{name}` is built-in.')
            return True
        command = self.find_command(space, name, follow_alias=False)
        if not command:
            await self.send_to_channel(trigger.channel, f'Unknown command in this space: `{name}`')
            return False
        if command.author == trigger.author.id:
            await self.send_to_channel(trigger.channel, f'You own the command: `{name}`')
            return True
        owner_user = await self.get_or_fetch_user(command.author, channel=trigger.channel)
        msg = f'The command `{name}` belongs to <@!{command.author}>.' if owner_user else f'The command `{name}` is currently unowned.'
        await self.send_to_channel(trigger.channel, msg)
        return True

    async def say(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str], processor: Callable[[str], str] = identity) -> bool:
        msg = f'Message may not be empty\nUsage: `{command_name}` <message>' if not command_predicate else processor(command_predicate)
        await self.send_to_channel(trigger.channel, msg)
        return command_predicate is not None

    def get_message_space(self, message: discord.Message) -> Space:
        if hasattr(message.channel, 'guild'):
            return self.get_guild_space(message.channel.guild.id)
        if hasattr(message.channel, 'recipient'):
            return self.get_dm_space(message.channel.recipient.id)
        if hasattr(message.channel, 'recipients'):
            return self.get_channel_space(message.channel.id)
        msg = f'Uknown space for message: {message.id}'
        self.logger.critical(msg)
        raise ValueError(msg)

    def get_dm_space(self, base_id: int) -> Space:
        space_id = f'dm_{base_id}'
        if space_id in self.spaces:
            return self.spaces[space_id]
        self.spaces[space_id] = DMSpace(client=self, base_id=base_id)
        return self.spaces[space_id]

    def get_channel_space(self, base_id: int) -> Space:
        space_id = f'chan_{base_id}'
        if space_id in self.spaces:
            return self.spaces[space_id]
        self.spaces[space_id] = ChannelSpace(client=self, base_id=base_id)
        return self.spaces[space_id]

    def get_guild_space(self, base_id: int) -> Space:
        space_id = f'guild_{base_id}'
        if space_id in self.spaces:
            return self.spaces[space_id]
        self.spaces[space_id] = GuildSpace(client=self, base_id=base_id)
        return self.spaces[space_id]

    def get_space(self, space_id: str) -> Space:
        if space_id in self.spaces:
            return self.spaces[space_id]
        if space_id.startswith('dm_'):
            base_id = int(removeprefix(space_id, 'dm_'))
            return self.get_dm_space(base_id)
        if space_id.startswith('chan_'):
            base_id = int(removeprefix(space_id, 'chan_'))
            return self.get_channel_space(base_id)
        if space_id.startswith('guild_'):
            base_id = int(removeprefix(space_id, 'guild_'))
            return self.get_guild_space(base_id)
        raise ValueError(f'Invalid space_id: {space_id}')

    def _load_space_overrides0(self) -> bool:
        for space_id in os.listdir('storage/'):
            space = self.get_space(space_id)
            space_json_fname = f'storage/{space_id}/space.json'
            if os.path.isfile(space_json_fname):
                with open(space_json_fname, 'r', encoding='UTF-8') as json_file:
                    space_json = json.load(json_file)
                    space.load_properties(space_json)
            commands = []
            if os.path.isdir(f'storage/{space_id}/commands/'):
                for command_json_fname in os.listdir(f'storage/{space_id}/commands/'):
                    with open(f'storage/{space_id}/commands/{command_json_fname}', encoding='UTF-8') as json_file:
                        try:
                            command_json = json.load(json_file)
                            commands += [command_json]
                        except json.decoder.JSONDecodeError:
                            self.logger.error(f'Corrupt command json: {command_json_fname} in {space_id}')
            if not space.load_commands(commands):
                self.logger.error(f'Unable to load commands from space: {space_id}')
                return False
        return True

    def load_space_overrides(self) -> bool:
        try:
            return self._load_space_overrides0()
        except IOError:
            self.logger.exception('Unable to load space overrides')
        return False

    def find_command(self, space: Space, command_name: str, follow_alias: bool = True) -> Optional[Command]:
        if command_name in self.builtin_command_dict:
            command = self.builtin_command_dict[command_name]
        elif command_name in space.custom_command_dict:
            command = space.custom_command_dict[command_name]
        else:
            return None
        return command.canonical() if follow_alias else command

    async def passthrough_command(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]):
        if not command_predicate:
            await self.send_to_channel(trigger.channel, f'Command name may not be empty\nUsage: `{command_name} <command_name> [command_args...]`')
            return False
        return await self.process_command(trigger, space, command_predicate)

    async def list_all_commands(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]):
        if not space.is_moderator(trigger.author):
            await self.send_to_channel(trigger.channel, 'Only moderators may do this.')
            return False
        builtin_command_string = '**Built-in Commands**'
        alias_command_string = '**Aliases**'
        for name, command in self.builtin_command_dict.items():
            # use patterns in python 3.10
            if command.command_type in ['function', 'simple']:
                builtin_command_string += f'\n`{name}`: {command.get_help()}'
            elif isinstance(command, CommandAlias):
                alias_command_string += f'\n`{name}`: {command.value.name}'
            else:
                self.logger.error(f'Invalid command type: {name}, {command.command_type}')
                return False
        response_string = f'{builtin_command_string}\n\n{alias_command_string}\n\n**Custom Commands**'
        if len(space.custom_command_dict) > 0:
            response_string += '```'
            for name in space.custom_command_dict:
                # max 2k characters
                # 1997 plus the ``` at the end gives 2k
                if len(response_string) + len(name) > 1997:
                    await self.send_to_channel(trigger.channel, f'{response_string[:-2]}```')
                    response_string = '```'
                response_string += f'{name}, '
            response_string = f'{response_string[:-2]}```'
        else:
            response_string += '\n*(There are no custom commands in this space.)*'
        await self.send_to_channel(trigger.channel, response_string)
        return True

    # The name has to be legal, or it will exception
    def get_property(self, space: Space, name: str, use_default: bool = True) -> Any:
        value = getattr(space, name)
        if value:
            return value
        if use_default:
            return self.default_properties[name]
        return None

    async def get_or_fetch_channel(self, channel_id) -> Optional[discord.abc.Messageable]:
        channel_obj = self.get_channel(channel_id)
        if channel_obj:
            return channel_obj
        try:
            channel_obj = await self.fetch_channel(channel_id)
        except discord.HTTPException:
            self.logger.exception(f'Could not fetch channel: {channel_id}')
            return None
        return channel_obj

    async def get_or_fetch_guild(self, guild_id) -> Optional[discord.Guild]:
        guild_obj = self.get_guild(guild_id)
        if guild_obj:
            return guild_obj
        try:
            guild_obj = await self.fetch_guild(guild_id)
        except discord.HTTPException:
            self.logger.exception(f'Could not fetch guild: {guild_id}')
            return None
        return guild_obj

    async def get_or_fetch_user(self, user_id, channel=None) -> Optional[Union[discord.User, discord.Member]]:
        if channel and hasattr(channel, 'guild'):
            return await self.get_or_fetch_member(channel.guild, user_id)
        user_obj = self.get_user(user_id)
        if user_obj:
            return user_obj
        try:
            user_obj = await self.fetch_user(user_id)
        except discord.HTTPException:
            self.logger.exception(f'Could not fetch user: {user_id}')
            return None
        return user_obj

    async def get_or_fetch_member(self, guild, user_id) -> Optional[discord.Member]:
        member_obj = guild.get_member(user_id)
        if member_obj:
            return member_obj
        try:
            member_obj = await guild.fetch_member(user_id)
        except discord.HTTPException:
            self.logger.exception(f'Could not fetch member: {user_id}')
            return None
        return member_obj

    async def process_command(self, trigger: discord.Message, space: Space, command_string: str) -> bool:
        command_name, command_predicate = split_command(command_string)
        if not command_name:
            return False
        command = self.find_command(space, command_name, follow_alias=True)
        if command:
            return await command.invoke(trigger, space, command_name, command_predicate)
        await self.send_to_channel(trigger.channel, f'Unknown command in this space: `{command_name}`')
        return False

    # wikitext stuff

    async def set_wikitext(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        if not space.is_moderator(trigger.author):
            await self.send_to_channel(trigger.channel, 'Only moderators may do this.')
            return False
        usage = f'Usage: `{command_name}` <enable/disable>'
        value, _ = split_command(command_predicate)
        if not value:
            await self.send_to_channel(trigger.channel, f'Choose enable or disable.\n{usage}')
            return False
        if re.match(r'^yes|on|true|enabled?$', value):
            new_value = True
        elif re.match(r'^no|off|false|disabled?$', value):
            new_value = False
        else:
            await self.send_to_channel(trigger.channel, f'Invalid enable/disable value.\n{usage}')
            return False
        space.wikitext = new_value
        success = space.save()
        msg = f'Wikitext for this space changed to `{new_value}`' if success else 'Unknown error when saving properties'
        await self.send_to_channel(trigger.channel, msg)
        return success

    async def reset_wikitext(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        if not space.is_moderator(trigger.author):
            await self.send_to_channel(trigger.channel, 'Only moderators may do this.')
            return False
        space.wikitext = None
        success = space.save()
        msg = f'Wikitext for this space reset to the default, which is `{self.default_properties["wikitext"]}`' if success else 'Unknown error when saving properties'
        await self.send_to_channel(trigger.channel, msg)
        return success

    async def handle_wiki_lookup(self, trigger: discord.Message, extra_wikis: List[str]):
        chunks = get_all_noncode_chunks(trigger.content)
        article_chunks = [re.findall(r'\[\[(.*?)\]\]', chunk) for chunk in chunks]
        articles = [article for chunk in article_chunks for article in chunk if len(article.strip()) > 0]
        if len(articles) > 0:
            await self.send_to_channel(trigger.channel, '\n'.join([lookup_wikis(article, extra_wikis=extra_wikis) for article in articles]))
            return True
        return False

    # events

    # return value
    # True: attempted to respond to the message
    # False: ignored the message
    async def handle_message(self, trigger: discord.Message) -> bool:
        if trigger.author == self.user:
            return False
        if trigger.author.bot:
            return False
        content = trigger.content.strip()
        space = self.get_message_space(trigger)
        prefix = self.get_property(space, 'command_prefix')
        if content.startswith(prefix):
            command_string = removeprefix(content, prefix).strip()
            await self.process_command(trigger, space, command_string)
            return True
        if self.get_property(space, 'wikitext'):
            return await self.handle_wiki_lookup(trigger, self.extra_wikis)
        return False

    # setup stuff

    def __init__(self, *args, bot_name: str, bot_storage_area: str = '~/.config/deep-blue-sky', **kwargs):

        self.bot_name = bot_name
        self.bot_dir = os.path.expanduser(f'{bot_storage_area}/{bot_name}')
        os.makedirs(self.bot_dir, mode=0o755, exist_ok=True)
        os.chdir(self.bot_dir)
        for subdir in 'feed', 'storage':
            os.makedirs(f'{self.bot_dir}/{subdir}', mode=0o755, exist_ok=True)

        self.logger = logging.getLogger('discord')
        self.logger.setLevel(logging.INFO)
        # pylint: disable=consider-using-with
        self.log_file = open('bot_output.log', mode='a', buffering=1, encoding='UTF-8')
        handler = logging.StreamHandler(stream=self.log_file)
        formatter = logging.Formatter(fmt='[{asctime}] {levelname}: {message}', style='{')
        formatter.converter = time.gmtime # type: ignore
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        intents = discord.Intents.default()
        # pylint: disable=assigning-non-slot
        intents.members = True
        super().__init__(*args, allowed_mentions=discord.AllowedMentions.none(), intents=intents, chunk_guilds_at_startup=True, **kwargs)
        for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT]:
            self.loop.add_signal_handler(sig, lambda sig = sig: asyncio.create_task(self.signal_handler(sig, self.loop)))

        builtin_list = [
            CommandFunction(name='help', value=self.send_help, helpstring='Print help messages'),
            CommandSimple(name='ping', value='pong', builtin=True, helpstring='Reply with pong'),
            CommandFunction(name='set-prefix', value=self.change_prefix, helpstring='Set the command prefix in this space'),
            CommandFunction(name='reset-prefix', value=self.reset_prefix, helpstring='Restore the command prefix in this space to the default value'),
            CommandFunction(name='set-wikitext', value=self.set_wikitext, helpstring='Enable or disable wikitext in this space'),
            CommandFunction(name='reset-wikitext', value=self.reset_wikitext, helpstring='Restore wikitext in this space to the default value'),
            CommandFunction(name='createcommand', value=self.create_command, helpstring='Create a new simple command in this space'),
            CommandFunction(name='removecommand', value=self.remove_command, helpstring='Remove a simple command and all of its aliases'),
            CommandFunction(name='updatecommand', value=self.update_command, helpstring='Change the value of a simple command'),
            CommandFunction(name='listcommands', value=self.list_commands, helpstring='List simple commands you own'),
            CommandFunction(name='takecommand', value=self.take_command, helpstring='Take ownership of a simple command'),
            CommandFunction(name='givecommand', value=self.give_command, helpstring='Give ownership of a simple command'),
            CommandFunction(name='command', value=self.passthrough_command, helpstring='Call a command (this is for backwards compatibility)'),
            CommandFunction(name='list-all-commands', value=self.list_all_commands, helpstring='List all commands in this space (this is spammy!)'),
            CommandFunction(name='whoowns', value=self.who_owns_command, helpstring='Report who owns a simple command'),
            CommandFunction(name='say', value=self.say, helpstring='prints the text back, like echo(1)'),
            CommandFunction(name='owo', value=functools.partial(self.say, processor=owoify), helpstring='pwints the text back, wike echo(1)'),
            CommandFunction(name='spongebob', value=functools.partial(self.say, processor=spongebob), helpstring='pRiNtS tHe TeXt BaCk, LiKe EcHo(1)'),
        ]

        self.builtin_command_dict = OrderedDict([(command.name, command) for command in builtin_list])

        alias_list = [
            CommandAlias(name='halp', value=self.builtin_command_dict['help']),
            CommandAlias(name='changeprefix', value=self.builtin_command_dict['set-prefix']),
            CommandAlias(name='change-prefix', value=self.builtin_command_dict['set-prefix']),
            CommandAlias(name='resetprefix', value=self.builtin_command_dict['reset-prefix']),
            CommandAlias(name='wikitext', value=self.builtin_command_dict['set-wikitext']),
            CommandAlias(name='setwikitext', value=self.builtin_command_dict['set-wikitext']),
            CommandAlias(name='resetwikitext', value=self.builtin_command_dict['reset-wikitext']),
            CommandAlias(name='newcommand', value=self.builtin_command_dict['createcommand']),
            CommandAlias(name='addcommand', value=self.builtin_command_dict['createcommand']),
            CommandAlias(name='addc', value=self.builtin_command_dict['createcommand']),
            CommandAlias(name='deletecommand', value=self.builtin_command_dict['removecommand']),
            CommandAlias(name='delc', value=self.builtin_command_dict['removecommand']),
            CommandAlias(name='renewcommand', value=self.builtin_command_dict['updatecommand']),
            CommandAlias(name='fixc', value=self.builtin_command_dict['updatecommand']),
            CommandAlias(name='commandlist', value=self.builtin_command_dict['listcommands']),
            CommandAlias(name='clist', value=self.builtin_command_dict['listcommands']),
            CommandAlias(name='listc', value=self.builtin_command_dict['listcommands']),
            CommandAlias(name='commandlist', value=self.builtin_command_dict['listcommands']),
            CommandAlias(name='c', value=self.builtin_command_dict['command']),
            CommandAlias(name='listallcommands', value=self.builtin_command_dict['list-all-commands']),
            CommandAlias(name='owner', value=self.builtin_command_dict['whoowns']),
            CommandAlias(name='clyde', value=self.builtin_command_dict['say']),
        ]

        self.builtin_command_dict.update(OrderedDict([(command.name, command) for command in alias_list]))
        self.default_properties: Dict[str, Any] = {
            'space_id' : 'default',
            'command_prefix' : '--',
            'wikitext' : False,
        }
        self.extra_wikis: List[str] = []
        self.spaces: Dict[str, Space] = {}
        self.load_space_overrides()

    # cleanup stuff

    async def cleanup(self):
        self.logger.info('Received signal, exiting gracefully')
        await self.change_presence(status=discord.Status.invisible, activity=None)
        await self.close()

    async def signal_handler(self, caught_signal, frame):
        try:
            await self.cleanup()
            cleanup_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            _ = [task.cancel() for task in cleanup_tasks]
            await asyncio.gather(*cleanup_tasks)
        finally:
            self.log_file.close()


    # connect logic
    
    # this is an event handler
    # because we subclass discord.Client
    async def on_ready(self):
        self.logger.info(f'Logged in as {self.user}')
        game = discord.Game(self.default_properties['command_prefix'] + 'help')
        await self.change_presence(status=discord.Status.online, activity=game)

    def run(self, token=None):

        if not token:
            with open('oauth_token', 'r', encoding='UTF-8') as token_file:
                token = token_file.read()            
        if not token or token == '':
            self.logger.critical('Error reading OAuth Token')
            sys.exit(1)

        self.logger.info(f'Beginning connection as {self.bot_name}')

        try:
            self.loop.run_until_complete(self.start(token, reconnect=True))
        finally:
            self.loop.close()
