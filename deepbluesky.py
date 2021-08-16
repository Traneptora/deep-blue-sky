#!/usr/bin/env python3

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
from typing import Any, Callable, Optional, Union
from typing import Dict, List, Set, Tuple

import discord
import requests

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

def lookup_tvtropes(article: str) -> Tuple[bool, Optional[str]]:
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
    return (True, result.url) if result.ok else (False, None)

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
        snowflake_input = int(snowflake_input)
    # intentionally not catching ValueError here, since string IDs should cast to int
    # TypeError means not a snowflake, string, or int, so probably an iterable
    except TypeError:
        pass

    try:
        snowflake_input = list(snowflake_input)
    # probably an integer
    except TypeError:
        snowflake_input = [snowflake_input]

    return [int(snowflake) for snowflake in snowflake_input]

def split_command(command_string: Optional[str]) -> Union[Tuple[str, Optional[str]], Tuple[None, None]]:
    if not command_string:
        return (None, None)
    name, predicate, *_ = *command_string.split(maxsplit=1), None, None
    name = name[:64].rstrip(':,').lower() if name else None
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
    chunks, _ = chunk_message(message_string, '```')
    chunks, _ = zip(*[chunk_message(chunk, '`') for chunk in chunks])
    # zip will zip this into a tuple
    # cast to list to return a proper list
    return [y for x in chunks for y in x]

class Space(abc.ABC):
    pass
class Command(abc.ABC):
    pass

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
            if author_id != trigger.author.id and not space.is_moderator(trigger.author) and await self.user_exists(author_id, trigger.channel):
                await self.send_to_channel(trigger.channel, f'The command `{name}` blongs to <@!{author_id}>. You cannot remove it.')
                return False
        success = True
        success_list = []
        while len(command_set) > 0:
            for name in list(command_set):
                command = space.custom_command_dict[name]
                command_set.update({alias.name for alias in command.aliases})
                if command.command_type == 'alias':
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
        if command.author != trigger.author.id and not space.is_moderator(trigger.author) and await self.user_exists(command.author, trigger.channel):
            await self.send_to_channel(trigger.channel, f'The command `{command.name}` blongs to <@!{command.author}>. You cannot update it.')
            return False
        lines = [x.strip() for x in [new_value] if x] + [attachment.url for attachment in trigger.attachments]
        if len(lines) == 0:
            await self.send_to_channel(trigger.channel, f'Command value may not be empty\n{usage}')
            return False
        new_value = '\n'.join(lines)
        command.value = new_value
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
            if author_id != trigger.author.id and not space.is_moderator(trigger.author) and await self.user_exists(author_id, trigger.channel):
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
        usage = f'Usage: `{command_name}` <command_names...>'
        return await self._give_command0(trigger, space, command_name, remainder=command_predicate, verb='take', participle='taken', usage=usage, give_id=trigger.author.id)

    async def give_command(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]) -> bool:
        usage = f'Usage: `{command_name}` <user_spec> <command_names...>'
        if not command_predicate:
            await self.send_to_channel(trigger.channel, f'Provide user and command name\n{usage}')
            return False
        match = re.match(r'[^#]+#[0-9]{4}(?=\s+)', command_predicate)
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
            elif command.command_type == 'alias':
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
        articles = [re.findall(r'\[\[(.*?)\]\]', chunk) for chunk in chunks]
        articles = [article for chunk in articles for article in chunk if len(article.strip()) > 0]
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

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('discord')
        self.logger.setLevel(logging.INFO)
        # pylint: disable=consider-using-with
        self.log_file = open('bot_output.log', mode='a', buffering=1, encoding='UTF-8')
        handler = logging.StreamHandler(stream=self.log_file)
        formatter = logging.Formatter(fmt='[{asctime}] {levelname}: {message}', style='{')
        formatter.converter = time.gmtime
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
            'command_prefix' : '--',
            'wikitext' : False,
            'space_id' : 'default',
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

    def run(self, *args, **kwargs):
        token = None
        with open('oauth_token', 'r', encoding='UTF-8') as token_file:
            token = token_file.read()
        if not token or token == '':
            self.logger.critical('Error reading OAuth Token')
            sys.exit(1)

        self.logger.info('Beginning connection.')

        try:
            self.loop.run_until_complete(self.start(token, reconnect=True))
        finally:
            self.loop.close()


class Space(abc.ABC):

    # pylint: disable=function-redefined

    def __init__(self, client: DeepBlueSky, space_type: str, base_id: int):
        self.client: DeepBlueSky = client
        self.base_id: int = base_id
        self.custom_command_dict: Dict[str, Command] = OrderedDict([])
        self.crtime: int = int(time.time())
        self.mtime: int = int(time.time())
        self.wikitext: Optional[bool] = None
        self.command_prefix: Optional[bool] = None
        self.space_type: str = space_type
        self.space_id: str = f'{space_type}_{base_id}'

    def __str__(self) -> str:
        return self.space_id

    def get_all_properties(self) -> Dict[str, Any]:
        return {attr: getattr(self, attr) for attr in list(self.client.default_properties.keys()) + ['crtime', 'mtime']}

    def save(self, update_mtime: bool = True) -> bool:
        if update_mtime:
            self.mtime = int(time.time())
        space_properties = self.get_all_properties()
        dirname = f'storage/{self.space_id}'
        try:
            os.makedirs(dirname, mode=0o755, exist_ok=True)
            with open(f'{dirname}/space.json', 'w', encoding='UTF-8') as json_file:
                json.dump(space_properties, json_file)
        except IOError:
            self.client.logger.exception(f'Unable to save space: {self.space_id}')
            return False
        return True

    def save_command(self, command_name: str, update_mtime: bool = True) -> bool:
        dirname=f'storage/{self.space_id}/commands'
        command_json_fname = f'{dirname}/{command_name}.json'
        try:
            os.makedirs(dirname, mode=0o755, exist_ok=True)
            if command_name in self.custom_command_dict:
                command = self.custom_command_dict[command_name]
                command.modification_time = int(time.time())
                with open(command_json_fname, 'w', encoding='UTF-8') as json_file:
                    json.dump(command.get_dict(), json_file)
            elif os.path.isfile(command_json_fname):
                os.remove(command_json_fname)
            return True
        except IOError:
            self.client.logger.exception(f'Unable to save command in space: {self.space_id}')
            return False

    def load_properties(self, property_dict: Dict[str, Any]):
        for attr in self.client.default_properties.keys():
            setattr(self, attr, property_dict.get(attr, None))
        # this mangles space_id so we re-set it
        self.space_id = f'{self.space_type}_{self.base_id}'
        for attr in ['crtime', 'mtime']:
            setattr(self, attr, property_dict.get(attr, int(time.time())))


    def load_command(self, command_dict: Dict[str, Any]) -> bool:
        # python 3.10: use patterns
        if command_dict['type'] != 'simple' and command_dict['type'] != 'alias':
            msg = f'Invalid custom command type: {command_dict["type"]}'
            self.client.logger.error(msg)
            raise ValueError(msg)

        author = command_dict['author']
        name = command_dict['name']
        crtime = command_dict['crtime']
        mtime = command_dict['mtime']
        value = command_dict['value']

        if command_dict['type'] == 'simple':
            command = CommandSimple(name=name, author=author, creation_time=crtime, modification_time=mtime, value=value)
        else:
            # command_type must equal 'alias'
            if value in self.client.builtin_command_dict:
                value = self.client.builtin_command_dict[value]
            elif value in self.custom_command_dict:
                value = self.custom_command_dict[value]
            else:
                self.client.logger.warning(f'cant add alias before its target. name: {name}, value: {value}')
                return False
            command = CommandAlias(name=name, author=author, creation_time=crtime, modification_time=mtime, value=value, builtin=False)
        self.custom_command_dict[name] = command
        return True

    def load_commands(self, command_dict_list: List[Dict[str, Any]]) -> bool:
        failed_all = False
        commands_to_add = command_dict_list[:]
        while len(commands_to_add) > 0 and not failed_all:
            failed_all = True
            for command_dict in commands_to_add[:]:
                if self.load_command(command_dict):
                    commands_to_add.remove(command_dict)
                    failed_all = False
        if failed_all:
            self.client.logger.error(f'Broken aliases detected in space: {self.space_id}')
        return not failed_all

    async def query_users(self, query: str) -> int:
        # user ID input
        try:
            user_id = int(query)
            return user_id
        except ValueError:
            pass

        # ping input
        match = re.match(r'^<@!?([0-9]+)>', query)
        if match:
            return int(match.group(1))

        # username input
        return await self._query_users0(query.lower())

    @abc.abstractmethod
    async def _query_users0(self, query: str) -> int:
        pass

    @abc.abstractmethod
    def is_moderator(self, user: discord.abc.User) -> bool:
        pass

# abc for DM and Channel spaces
class PrivateSpace(Space):

    def is_moderator(self, user: discord.abc.User) -> bool:
        return True

    def _query_userlist(self, userlist: Set[discord.User], query: str) -> int:
        user_found = -1
        query = query.lower()
        for user in frozenset(userlist).union({self.client.user}):
            username = user.username.lower()
            fullname = username + '#' + user.discriminator
            displayname = user.display_name.lower()
            if fullname.startswith(query) or displayname.startswith(query):
                if user_found >= 0:
                    return -2
                user_found = user.id
        return user_found

class DMSpace(PrivateSpace):

    def __init__(self, client: DeepBlueSky, base_id: int):
        super().__init__(client=client, space_type='dm', base_id=base_id)
        self.recipient: Optional[discord.User] = None

    async def get_recipient(self) -> discord.User:
        if self.recipient:
            return self.recipient
        recipient = self.client.get_or_fetch_user(self.base_id)
        if not recipient:
            msg = f'Cannot find user: {self.base_id}'
            self.client.logger.critical(msg)
            raise RuntimeError(msg)
        self.recipient = recipient
        return self.recipient

    async def _query_users0(self, query: str) -> int:
        return self._query_userlist({await self.get_recipient()}, query)

class ChannelSpace(PrivateSpace):

    def __init__(self, client: DeepBlueSky, base_id: int):
        super().__init__(client=client, space_type='chan', base_id=base_id)
        self.channel: Optional[discord.GroupChannel] = None

    async def get_channel(self) -> discord.GroupChannel:
        if self.channel:
            return self.channel
        channel = self.client.get_or_fetch_channel(self.base_id)
        if not channel:
            msg = f'Cannot find channel: {self.base_id}'
            self.client.logger.critical(msg)
            raise RuntimeError(msg)
        if channel is not discord.GroupChannel:
            msg = f'Channel is not a GroupChannel: {self.base_id}'
            self.client.logger.critical(msg)
            raise RuntimeError(msg)
        self.channel = channel
        return self.channel

    async def _query_users0(self, query: str) -> int:
        return self._query_userlist(frozenset((await self.get_channel()).recipients), query)


class GuildSpace(Space):

    def __init__(self, client: DeepBlueSky, base_id: int):
        super().__init__(client=client, space_type='guild', base_id=base_id)
        self.guild: Optional[discord.Guild] = None

    async def get_guild(self) -> discord.Guild:
        if self.guild:
            return self.guild
        guild = await self.client.get_or_fetch_guild(self.base_id)
        if not guild:
            msg = f'Cannot find guild: {self.base_id}'
            self.client.logger.critical(msg)
            raise RuntimeError(msg)
        self.guild = guild
        return self.guild

    def is_moderator(self, user: discord.abc.User) -> bool:
        return hasattr(user, 'guild_permissions') and user.guild_permissions.kick_members

    async def _query_users0(self, query: str) -> int:
        member_list = await (await self.get_guild()).query_members(query=query)
        if len(member_list) == 0:
            return -1
        if len(member_list) > 1:
            return -2
        return member_list[0].id

class CommandAlias(Command):
    pass

class Command(abc.ABC):

    # pylint: disable=function-redefined

    def __init__(self, name: str, author: Optional[int], command_type: str, creation_time: Optional[int], modification_time: Optional[int]):
        self.name = name
        self.author = author
        self.aliases = []
        self.command_type = command_type
        self.creation_time = creation_time
        self.modification_time = modification_time
        self.space = None

    # the returned value is the success bool
    # the command message has already been sent to the channel
    @abc.abstractmethod
    async def _invoke0(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        pass

    async def invoke(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        if self.space and self.space != space:
            # This should not happen
            space.client.logger.error(f'Command {self.name} owned by another space: {self.space}, not {space}')
            return False
        if await self.can_call(trigger, space):
            try:
                result = await self._invoke0(trigger, space, name_used, command_predicate)
                if result:
                    space.client.logger.info(f'Command succeeded, author: {trigger.author.id}, name: {self.name}')
                else:
                    space.client.logger.info(f'Command failed, author: {trigger.author.id}, name: {self.name}')
            # pylint: disable=broad-except
            except Exception as ex:
                space.client.logger.critical(f'Unexpected exception during command invocation: {str(ex)}', exc_info=True)
                return False
        else:
            space.client.logger.warning(f'User {trigger.author.id} illegally attempted command {self.name}')
            return False

    @abc.abstractmethod
    def get_help(self) -> str:
        pass

    @abc.abstractmethod
    def is_builtin(self) -> bool:
        pass

    async def can_call(self, trigger: discord.Message, space: Space) -> bool:
        return True

    def canonical(self, path: Optional[list] = None) -> Command:
        return self

    def follow(self) -> Command:
        return self

    def get_dict(self) -> Dict[str, Any]:
        return {
            'type' : self.command_type,
            'name' : self.name,
            'author' : self.author,
            'crtime' : self.creation_time,
            'mtime' : self.modification_time,
            **self._get_dict0()
        }

    @abc.abstractmethod
    def _get_dict0(self) -> Dict[str, Any]:
        pass

    def __eq__(self, other) -> bool:
        if not other:
            return False
        if id(self) == id(other):
            return True
        return self.name == other.name and self.space == other.space and self.command_type == other.command_type

    def __hash__(self) -> int:
        return hash((self.name, self.space, self.command_type))

class CommandSimple(Command):

    def __init__(self, name: str, value: str, author: Optional[int] = None,  creation_time: Optional[int] = None, modification_time: Optional[int] = None, builtin: bool = False, helpstring: Optional[str] = None):
        super().__init__(name=name, author=author, command_type='simple', creation_time=creation_time, modification_time=modification_time)
        self.value = value
        self.builtin = builtin
        self.helpstring = helpstring if helpstring else 'a simple command replies with its value'

    # override
    async def _invoke0(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        try:
            await space.client.send_to_channel(trigger.channel, self.value)
        except discord.Forbidden:
            space.client.logger.error(f'Insufficient permissions to send to channel. id: {trigger.channel.id}, name: {self.name}')
            return False
        return True

    def get_help(self) -> str:
        return f'Reply with {self.value}' if self.is_builtin() else self.helpstring

    def is_builtin(self) -> bool:
        return self.builtin

    def _get_dict0(self) -> Dict[str, Any]:
        return {'value': self.value}

class CommandAlias(Command):

    # pylint: disable=function-redefined

    def __init__(self, name: str, value: Command, author: Optional[int] = None, creation_time: Optional[int] = None, modification_time: Optional[int] = None, builtin: Optional[bool] = None):
        super().__init__(name=name, author=author, command_type='alias', creation_time=creation_time, modification_time=creation_time)
        self.value = value
        self.value.aliases.append(self)
        self.builtin = builtin if builtin is not None else self.value.is_builtin()

    async def _invoke0(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        return await self.canonical().invoke(trigger, space, name_used, command_predicate)

    def get_help(self) -> str:
        return f'alias for `{self.value.name}`'

    def is_builtin(self) -> bool:
        return self.builtin

    # Axiom of Regularity
    # https://en.wikipedia.org/wiki/Axiom_of_regularity
    # prevent infinte alias loops
    # This should never fail because alias creation requires another command object
    # but failsafes are good
    def check_regularity(self, path: Optional[list] = None) -> bool:
        if not path:
            path = []
        if self in path:
            return False
        return self.value is not CommandAlias or self.value.check_regularity(path + [self])

    def canonical(self, path: Optional[list] = None) -> Command:
        if not path:
            path = []
        if self in path:
            raise RuntimeError(f'alias cycle detected: {self.name}, {path}')
        return self.value.canonical(path + [self])

    def follow(self) -> Command:
        return self.value

    def _get_dict0(self) -> Dict[str, Any]:
        return {'value': self.value.name}

class CommandFunction(Command):

    def __init__(self, name: str, value: Callable[[discord.Message, Space, str, str], bool], helpstring: str):
        super().__init__(name=name, author=None, command_type='function', creation_time=None, modification_time=None)
        self.value = value
        self.helpstring = helpstring

    def is_builtin(self) -> bool:
        return True

    async def _invoke0(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        return await self.value(trigger, space, name_used, command_predicate)

    def get_help(self) -> str:
        return self.helpstring

    def _get_dict0(self) -> Dict[str, Any]:
        raise RuntimeError('cannot get dict for functional command')
