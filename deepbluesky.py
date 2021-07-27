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

import discord
import requests
from discord.ext import tasks

def identity(input: Any) -> Any:
    return input

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

def snowflake_list(snowflake_input: Optional[Union[str, discord.abc.Snowflake, int, list[Union[str, discord.abc.Snowflake, int]]]]) -> list[int]:
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

def split_command(command_string):
    args = command_string.split(maxsplit=1)
    if len(args) == 0:
        return (None, None)
    command_name = args[0]
    command_predicate = args[1] if len(args) > 1 else None
    command_name = command_name[0:64].rstrip(':').lower()
    return (command_name, command_predicate)

class DeepBlueSky(discord.Client):

    async def send_to_channel(self, channel: discord.abc.Messageable, message_content: str, ping_user=None, ping_roles=None):
        ping_user = [self.get_or_fetch_user(user) for user in snowflake_list(ping_user)]
        if hasattr(channel, 'guild'):
            ping_roles = [channel.guild.get_role(role) for role in snowflake_list(ping_roles)]
        else:
            ping_roles = []
        await channel.send(message_to_send, allowed_mentions=discord.AllowedMentions(users=ping_user, roles=ping_roles))

    # command functions
    
    async def send_help(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]):
        wanted_help, _ = split_command(command_predicate) if command_predicate else (None, None)
        if not wanted_help:
            if space.is_moderator(trigger.author):
                help_string = '\n'.join([f'`{command.name}`: {command.get_help()}' for command in self.builtin_command_dict.values() if command.command_type != 'alias'])
            else:
                help_string = 'Please send me a direct message.'
        else:
            if wanted_help in self.builtin_command_dict:
                help_string = self.builtin_command_dict[wanted_help].get_help()
            else:
                help_string = f'Cannot provide help in this space for: `{wanted_help}`'
        await self.send_to_channel(trigger.channel, help_string)

    async def change_prefix(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]):
        if not space.is_moderator(trigger.author):
            await self.send_to_channel(trigger.channel, 'Only moderators may do this.')
            return False
        if not command_predicate:
            await self.send_to_channel(trigger.channel, f'New prefix may not be empty\nUsage: `{command_name} <new_prefix>`')
            return False
        new_prefix, split_predicate = split_command(command_predicate)
        if split_predicate:
            await self.send_to_channel(trigger.channel, f'Invalid trailing arguments: `{split_predicate}`\nUsage: `{command_name} <new_prefix>`')
            return False
        if not new_prefix:
            await self.send_to_channel(trigger.channel, f'New prefix may not be empty\nUsage: `{command_name} <new_prefix>`')
            return False
        if not re.match(r'[a-z0-9_\-!.\.?]+', new_prefix):
            await self.send_to_channel(trigger.channel, f'Invalid prefix: `{new_prefix}`\nOnly ASCII alphanumeric characters or `-_!.?` permitted\nUsage: `{command_name} <new_prefix>`')
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
            await self.send_to_channel(message.channel, msg)
            return False
        await self.send_to_channel(message.channel, f'Prefix for this space changed to `{new_prefix}`')
        return True

    async def reset_prefix(self, message: discord.Message, space_id, command_name, command_predicate):
        if not self.is_moderator(message.author):
            await self.send_to_channel(message.channel, 'Only moderators may do this.')
            return False
        if command_predicate:
            await self.send_to_channel(message.channel, f'Invalid trailing arguments: `{command_predicate}`\nUsage: `{command_name}`')
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
            await self.send_to_channel(message.channel, msg)
            return False
        await self.send_to_channel(message.channel, f'Prefix for this space reset to the default, which is `{self.default_properties["command_prefix"]}`')
        return True

    async def create_command(self, message: discord.Message, space_id, command_name, command_predicate):
        if not command_predicate:
            await self.send_to_channel(message.channel, f'Command name may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
            return False
        new_name, new_value = split_command(command_predicate)
        if not re.match(r'[a-z0-9_\-!\.?]+', new_name):
            await self.send_to_channel(message.channel, f'Invalid command name: {new_name}\nOnly ASCII alphanumeric characters or `-_!.?` permitted\nUsage: `{command_name} <command_name> <command_value | attachment>`')
            return False
        if self.find_command(space_id, new_name, follow_alias=False):
            await self.send_to_channel(message.channel, f'The command `{new_name}` already exists in this space. Try using `updatecommand` instead.')
            return False
        if not new_value: 
            if len(message.attachments) > 0:
                new_value = message.attachments[0].url
            else:
                await self.send_to_channel(message.channel, f'Command value may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
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
            await self.send_to_channel(message.channel, msg)
            return False
        await self.send_to_channel(message.channel, f'Command added successfully. Try it with: `{self.get_in_space(space_id, "command_prefix")}{new_name}`')
        return True

    async def remove_command(self, message: discord.Message, space_id, command_name, command_predicate):
        if not command_predicate:
            await self.send_to_channel(message.channel, f'Command name may not be empty\nUsage: `{command_name} <command_name>`')
            return False
        goodbye_name, split_predicate = split_command(command_predicate)
        if split_predicate:
            await self.send_to_channel(message.channel, f'Invalid trailing arguments: `{split_predicate}`\nUsage: `{command_name} <command_name>`')
            return False
        if self.find_command('default', goodbye_name, follow_alias=False, use_default=False):
            await self.send_to_channel(message.channel, f'Built-in commands cannot be removed.')
            return False
        command = self.find_command(space_id, goodbye_name, follow_alias=False)
        if not command:
            await self.send_to_channel(message.channel, f'The command `{goodbye_name}` does not exist in this space.')
            return False
        if not self.is_moderator(message.author) and command['author'] != message.author.id:
            owner_user = await self.get_or_fetch_user(command['author'], channel=message.channel)
            if owner_user:
                await self.send_to_channel(message.channel, f'The command `{goodbye_name}` belongs to <@!{command["author"]}>. You cannot remove it.')
                return False

        old_command = self.space_overrides[space_id]['commands'].pop(goodbye_name)
        try:
            self.save_command(space_id, goodbye_name)
        except IOError as error:
            self.space_overrides[space_id]['commands'][goodbye_name] = old_command
            msg = 'Unknown error when removing command'
            self.logger.exception(msg)
            await self.send_to_channel(message.channel, msg)
            return False
        await self.send_to_channel(message.channel, f'Command `{goodbye_name}` removed successfully.')
        return True

    async def update_command(self, message: discord.Message, space_id, command_name, command_predicate):
        if not command_predicate:
            await self.send_to_channel(message.channel, f'Command name may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
            return False
        new_name, new_value = split_command(command_predicate)
        if not re.match(r'[a-z0-9_\-!\.?]+', new_name):
            await self.send_to_channel(message.channel, f'Invalid command name: {new_name}\nOnly ASCII alphanumeric characters or `-_!.?` permitted\nUsage: `{command_name} <command_name> <command_value | attachment>`')
            return False
        if self.find_command('default', new_name, follow_alias=False, use_default=False):
            await self.send_to_channel(message.channel, f'Built-in commands cannot be updated.')
            return False
        command = self.find_command(space_id, new_name, follow_alias=False)
        if not command:
            await self.send_to_channel(message.channel, f'The command `{new_name}` does not exist in this space. Create it with `createcommand` instead.')
            return False
        if not self.is_moderator(message.author) and command['author'] != message.author.id:
            owner_user = await self.get_or_fetch_user(command['author'], channel=message.channel)
            if owner_user:
                await self.send_to_channel(message.channel, f'The command `{new_name}` belongs to <@!{command["author"]}>. You cannot update it.')
                return False

        if not new_value:
            if len(message.attachments) > 0:
                new_value = message.attachments[0].url
            else:
                await self.send_to_channel(message.channel, f'Command value may not be empty\nUsage: `{command_name} <command_name> <command_value | attachment>`')
                return False

        old_value = command['value']
        command['value'] = new_value
        try:
            self.save_command(space_id, new_name)
        except IOError as error:
            command['value'] = old_value
            msg = 'Unknown error when updating command'
            self.logger.exception(msg)
            await self.send_to_channel(message.channel, msg)
            return False
        await self.send_to_channel(message.channel, f'Command updated successfully. Try it with: `{self.get_in_space(space_id, "command_prefix")}{new_name}`')
        return True

    async def list_commands(self, message: discord.Message, space_id, command_name, command_predicate):
        if command_predicate:
            await self.send_to_channel(message.channel, f'Invalid trailing arguments: `{command_predicate}`\nUsage: `{command_name}`')
            return False
        if space_id not in self.space_overrides:
            self.space_overrides[space_id] = { 'id' : space_id }
        command_list = self.space_overrides[space_id].get('commands', {})
        owned_commands = []
        for custom_command in command_list:
            if command_list[custom_command]['author'] == message.author.id:
                owned_commands.append(custom_command)
        if len(owned_commands) == 0:
            await self.send_to_channel(message.channel, 'You do not own any commands in this space.')
        else:
            await self.send_to_channel(message.channel, f'You own the following commands:\n```{", ".join(owned_commands)}```')

    async def take_command(self, message: discord.Message, space_id, command_name, command_predicate):
        if not command_predicate:
            await self.send_to_channel(message.channel, f'Command name may not be empty\nUsage: `{command_name} <command_name>`')
            return False
        take_name, split_predicate = split_command(command_predicate)
        if split_predicate:
            await self.send_to_channel(message.channel, f'Invalid trailing arguments: `{split_predicate}`\nUsage: `{command_name} <command_name>`')
            return False
        if self.find_command('default', take_name, follow_alias=False, use_default=False):
            await self.send_to_channel(message.channel, f'Built-in commands cannot be taken.')
            return False
        command = self.find_command(space_id, take_name, follow_alias=False)
        if not command:
            await self.send_to_channel(message.channel, f'That command does not exist in this space.')
            return False
        if command['author'] == message.author.id:
            await self.send_to_channel(message.channel, f'You already own the command `{take_name}`.')
            return False
        if not self.is_moderator(message.author):
            owner_user = await self.get_or_fetch_user(command['author'], channel=message.channel)
            if owner_user:
                await self.send_to_channel(message.channel, f'The command `{take_name}` belongs to <@!{command["author"]}>. You cannot take it.')
                return False
        old_author = command['author']
        command['author'] = message.author.id
        try:
            self.save_command(space_id, take_name)
        except IOError as error:
            command['author'] = old_author
            msg = 'Unknown error when taking command'
            self.logger.exception(msg)
            await self.send_to_channel(message.channel, msg)
            return False
        await self.send_to_channel(message.channel, f'Command ownership transfered successfully. You now own `{take_name}`.')
        return True

    async def who_owns_command(self, message, space_id, command_name, command_predicate):
        if not command_predicate:
            await self.send_to_channel(message.channel, f'Command name may not be empty\nUsage: `{command_name} <command_name>`')
            return False
        who_name, split_predicate = split_command(command_predicate)
        if split_predicate:
            await self.send_to_channel(message.channel, f'Invalid trailing arguments: `{split_predicate}`\nUsage: `{command_name} <command_name>`')
            return False
        if self.find_command('default', who_name, follow_alias=True, use_default=False):
            await self.send_to_channel(message.channel, f'The command `{who_name}` is built-in.')
            return True
        command = self.find_command(space_id, who_name, follow_alias=False)
        if not command:
            await self.send_to_channel(message.channel, f'The command `{who_name}` does not exist in this space.')
            return False
        if command['author'] == message.author.id:
            await self.send_to_channel(message.channel, f'You own the command: `{who_name}`')
            return True
        owner_user = await self.get_or_fetch_user(command['author'], channel=message.channel)
        if owner_user:
            await self.send_to_channel(message.channel, f'The command `{who_name}` belongs to <@!{command["author"]}>.')
            return True
        else:
            await self.send_to_channel(message.channel, f'The command `{who_name}` is currently unowned.')
            return True

    async def say(self, message, space_id, command_name, command_predicate, processor=identity):
        if not command_predicate:
            await self.send_to_channel(message.channel, f'Message may not be empty\nUsage: `{command_name} <message>`')
            return False
        else:
            await self.send_to_channel(message.channel, processor(command_predicate))
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

    def get_message_space(self, message: discord.Message) -> Space:
        if hasattr(message.channel, 'guild'):
            space_id = f'guild_{message.channel.guild.id}'
        elif hasattr(message.channel, 'recipient'):
            space_id = f'dm_{message.channel.recipient.id}'
        else:
            space_id = f'chan_{message.channel.id}'
        return self.get_space(space_id)

    def get_space(self, space_id) -> Space:
        if space_id in self.spaces: return self.spaces[space_id]

        if space_id.startswith('dm_'):
            try:
                recipient_id = int(space_id[len('dm_'):])
            except ValueError ex:
                self.logger.exception(f'Invalid space_id: {space_id}')
                raise ex
            self.spaces[space_id] = DMSpace(client=self, recipient_id=recipient_id)
        elif space_id.startswith('chan_'):
            try:
                channel_id = int(space_id[len('chan_'):])
            except ValueError ex:
                self.logger.exception(f'Invalid space_id: {space_id}')
                raise ex
            self.spaces[space_id] = ChannelSpace(client=self, channel_id=channel_id)
        elif space_id.startswith('guild_'):
            try:
                guild_id = int(space_id[len('guild_'):])
            except ValueError ex:
                self.logger.exception(f'Invalid space_id: {space_id}')
                raise ex
            self.space[space_id] = GuildSpace(client=self, guild_id=guild_id)
        else:
            raise ValueError(f'Invalid space_id: {space_id}')

        return self.spaces[space_id]

    def load_space_overrides(self):
        try:
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
                            command_json = json.load(json_file)
                            commands += [command_json]
                if not space.load_commands(commands):
                    self.logger.error(f'Unable to load commands from space: {space_id}')
                    return False
        except IOError as error:
            self.logger.exception('Unable to load space overrides')
            return False
        return True

    def is_moderator(self, user):
        if hasattr(user, 'guild_permissions'):
            return user.guild_permissions.kick_members
        else:
            return True

    def find_command(self, space: Space, command_name: str, follow_alias: bool = True):
        if command_name in self.builtin_command_dict:
            command = self.builtin_command_dict[command_name]
        elif command_name in space.custom_command_dict:
            command = space.custom_command_dict[command_name]
        else:
            return None
        return command.canonical() if follow_alias else command

    async def passthrough_command(self, message, space_id, command_name, command_predicate):
        if not command_predicate:
            await self.send_to_channel(message.channel, f'Command name may not be empty\nUsage: `{command_name} <command_name> [command_args...]`')
            return False
        return await self.process_command(message, space_id, command_predicate)

    async def list_all_commands(self, trigger: discord.Message, space: Space, command_name: str, command_predicate: Optional[str]):
        if await space.is_moderator(trigger.author.id):
            await self.send_to_channel(message.channel, f'Only moderators may do this.')
            return False
        if command_predicate:
            await self.send_to_channel(message.channel, f'Invalid trailing arguments: `{command_predicate}`\nUsage: `{command_name}`')
            return False
        builtin_command_string = '**Built-in Commands**'
        alias_command_string = '**Aliases**'
        for name, command in self.builtin_command_dict.items():
            if command.command_type == 'function'
                builtin_command_string += f'\n`{name}`: {command.get_help()}'
            elif command.command_type == 'alias':
                alias_command_string += f'\n`{name}`: {str(command.value)}'
            elif command.command_type == 'simple':
                builtin_command_string += f'\n`{name}`: Reply with `{command["value"]}`'
            else:
                self.logger.error(f'Invalid command type: {name}, {command.command_type}')
                return False
        response_string = f'{builtin_command_string}\n\n{alias_command_string}\n\n**Custom Commands**'
        if len(space.custom_command_dict) > 0:
            response_string += '```'
            for name in space.custom_command_dict:
                # max 2k characters
                # 1997 includes the ``` at the end
                if len(response_string) + len(name) > 1997:
                    await self.send_to_channel(trigger.channel, f'{response_string[:-2]}```')
                    response_string = '```'
                response_string += f'{name}, '
            response_string = f'{response_string[:-2]}```'
        else:
            response_string += '\n(There are no custom commands in this space.)'
        await self.send_to_channel(trigger.channel, response_string)
        return True

    async def get_or_fetch_channel(self, channel_id):
        channel_obj = self.get_channel(channel_id)
        if channel_obj: return channel_obj

        try:
            channel_obj = await self.fetch_channel(channel_id)
        except discord.HTTPException:
            self.logger.exception(f'Could not fetch channel: {channel_id}')
            return None
        return channel_obj

    async def get_or_fetch_guild(self, guild_id):
        guild_obj = self.get_guild(guild_id)
        if guild_obj: return guild_obj

        try:
            guild_obj = await self.fetch_guild(guild_id)
        except discord.HTTPException:
            self.logger.exception(f'Could not fetch guild: {guild_id}')
            return None
        return guild_obj

    async def get_or_fetch_user(self, user_id, channel=None):
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

    async def get_or_fetch_member(self, guild, user_id):
        member_obj = guild.get_member(user_id)
        if member_obj:
            return member_obj
        try:
            member_obj = await guild.fetch_member(user_id)
        except discord.HTTPException:
            self.logger.exception(f'Could not fetch member: {member_id}')
            return None
        return member_obj

    async def process_command(self, trigger: discord.Message, space: Space, command_string: str) -> bool:
        command_name, command_predicate = split_command(command_string)
        if not command_name:
            return False
        command = self.find_command(space, command_name, follow_alias=True)
        if command:
            return await command.invoke(trigger, space, command_name, command_predicate)
        else:
            await self.send_to_channel(trigger.channel, f'Unknown command in this space: `{command_name}`')
            return False


    # wikitext stuff
    
    async def set_wikitext(self, message, space_id, command_name, command_predicate):
        if not self.is_moderator(message.author):
            await self.send_to_channel(message.channel, 'Only moderators may do this.')
            return False
        if not command_predicate:
            await self.send_to_channel(message.channel, f'Choose enable or disable.\nUsage: `{command_name} <enable/disable>`')
            return False
        new_enabled, split_predicate = split_command(command_predicate)
        if split_predicate:
            await self.send_to_channel(message.channel, f'Invalid trailing predicate: `{split_predicate}`\nUsage: `{command_name} <enable/disable>`')
            return False
        if not new_enabled:
            await self.send_to_channel(message.channel, f'Choose enable or disable.\nUsage: `{command_name} <enable/disable>`')
            return False
        if re.match(r'yes|on|true|enabled?', new_enabled):
            new_value = True
        elif re.match(r'no|off|false|disabled?', new_enabled):
            new_value = False
        else:
            await self.send_to_channel(message.channel, f'Invalid enable/disable value: `{new_enabled}`\nUsage: `{command_name} <enable/disable>`')
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
            await self.send_to_channel(message.channel, msg)
            return False
        await self.send_to_channel(message.channel, f'Wikitext for this space changed to `{new_value}`')
        return True

    async def reset_wikitext(self, message, space_id, command_name, command_predicate):
        if not self.is_moderator(message.author):
            await self.send_to_channel(message.channel, 'Only moderators may do this.')
            return False
        if command_predicate:
            await self.send_to_channel(message.channel, f'Invalid trailing predicate: `{command_predicate}`\nUsage: `{command_name}`')
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
            await self.send_to_channel(message.channel, msg)
            return False
        await self.send_to_channel(message.channel, f'Wikitext for this space reset to the default, which is `{self.default_properties["wikitext"]}`')
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
            location = self.relative_to_absolute_location(request.headers['location'], server + query)
            return (True, location)
        result.encoding = 'UTF-8'
        if re.search(r"<div>Inexact title\. See the list below\. We don't have an article named <b>{}</b>/{}, exactly\. We do have:".format(namespace, title), result.text, flags=re.IGNORECASE):
            return (False, result.url)
        else:
            if result.ok:
                return (True, result.url)
            else:
                return (False, None)

    def relative_to_absolute_location(self, location, query_url):
        query_url = re.sub(r'\?.*$', '', query_url)
        if location.startswith('/'):
            server = re.sub(r'^([a-zA-Z]+://[^/]*)/.*$', r'\1', query_url)
            return server + location
        elif re.search(r'^[a-zA-Z]+://', location):
            return location
        else:
            return re.sub(r'^(([^/]*/)+)[^/]*', r'\1', query_url) + '/' + location;

    def lookup_mediawiki(self, mediawiki_base, article):
        parts = article.split('/')
        parts = [re.sub(r'^\s*([^\s]+(\s+[^\s]+)*)\s*$', r'\1', part) for part in parts]
        parts = [re.sub(r'\s', r'_', part) for part in parts]
        article= '/'.join(parts)
        params = { 'title' : 'Special:Search', 'go' : 'Go', 'ns0' : '1', 'search' : article }
        result = requests.head(mediawiki_base, params=params)
        if 'location' in result.headers:
            location = self.relative_to_absolute_location(result.headers['location'], mediawiki_base)
            if ':' in location[7:]:
                second_result = requests.head(location)
                if second_result.ok and 'last-modified' in second_result.headers:
                    return location
                else:
                    return None
            else:
                return location
        else:
            return None

    def lookup_wikis(self, article, extra_wikis=None):
        if extra_wikis:
            for wiki in extra_wikis:
                wiki_url = self.lookup_mediawiki(wiki, article)
                if wiki_url: return wiki_url
        success, tv_url = self.lookup_tvtropes(article.strip())
        if success:
            return tv_url
        wiki_url = self.lookup_mediawiki('https://en.wikipedia.org/w/index.php', article)
        if wiki_url:
            return wiki_url
        if tv_url and not extra_wikis:
            return f'Inexact Title Disambiguation Page Found:\n{tv_url}'
        else:
            return f'Unable to locate article: `{article}`'

    def chunk_message(self, message_string, chunk_delimiter):
        chunks = message_string.split(chunk_delimiter)
        if len(chunks) % 2 == 0:
            noncode_chunks = chunks[::2]
            code_chunks = chunks[1:-1:2]
            final_noncode_chunks = chunks[-1:]
        else:
            noncode_chunks = chunks[::2]
            code_chunks = chunks[1::2]
            final_noncode_chunks = []
        return (noncode_chunks, final_noncode_chunks, code_chunks)

    def assemble_message(self, noncode_chunks, code_chunks, chunk_delimiter):
        if (len(noncode_chunks) + len(code_chunks)) % 2 == 0:
            chunk_interleave = [val for pair in zip(noncode_chunks[:-2], code_chunks) for val in pair] + noncode_chunks[-2:]
        else:
            chunk_interleave = [val for pair in zip(noncode_chunks[:-1], code_chunks) for val in pair] + noncode_chunks[-1:]
        return chunk_delimiter.join(chunk_interleave)

    def get_all_noncode_chunks(self, message_string):
        chunks, final_chunks, _ = self.chunk_message(message_string, '```')
        for chunk in chunks + final_chunks:
            chunks_again, final_chunks_again, _ = self.chunk_message(chunk, '`')
            yield chunk_again for chunk_again in chunks_again + final_chunks_again

    # def _escape_ping_block(self, block_chunk):
    #     inline_noncode_chunks, inline_final_chunks, inline_code_chunks = self.chunk_message(block_chunk, '`')
    #     if len(inline_final_chunks) > 0:
    #         # Discord handles pings poorly, so we have to escape inside code blocks if we have mismatched delimiters
    #         inline_code_chunks = [re.sub(r'<?@(everyone|here|[^`0-9]?[0-9]+)>?', r'<@ \1>', string, flags=re.IGNORECASE) for string in inline_code_chunks]
    #     inline_noncode_chunks += inline_final_chunks
    #     inline_noncode_chunks = [re.sub(r'<?@(everyone|here|[^`0-9]?[0-9]+)>?', r'<@ \1>', string, flags=re.IGNORECASE) for string in inline_noncode_chunks]
    #     assembled_block = self.assemble_message(inline_noncode_chunks, inline_code_chunks, '`')
    #     return assembled_block

    # def _escape_ping_block(self, block_chunk):
    #     return re.sub(r'<?@(everyone|here|[^`0-9]?[0-9]+)>?', r'<@ \1>', block_chunk, flags=re.IGNORECASE)

    # def escape_pings(self, message_string):
    #     block_noncode_chunks, block_final_chunks, block_code_chunks = self.chunk_message(message_string, '```')
    #     if len(block_final_chunks) > 0:
    #         # Discord handles pings poorly, so we have to escape inside code blocks if we have mismatched delimiters
    #         block_code_chunks = [re.sub(r'<?@(everyone|here|[^`0-9]?[0-9]+)>?', r'<@ \1>', block_chunk, flags=re.IGNORECASE) for block_chunk in block_code_chunks]
    #     block_noncode_chunks += block_final_chunks
    #     block_noncode_chunks = [self._escape_ping_block(block_chunk) for block_chunk in block_noncode_chunks]
    #     ret = self.assemble_message(block_noncode_chunks, block_code_chunks, '```')
    #     return ret

    async def handle_wiki_lookup(self, message, extra_wikis=None):
        chunks = self.get_all_noncode_chunks(message.content)
        articles = [re.findall(r'\[\[(.*?)\]\]', chunk) for chunk in chunks]
        articles = [article for chunk in articles for article in chunk if len(article.strip()) > 0]
        if len(articles) > 0:
            await self.send_to_channel(message.channel, '\n'.join([self.lookup_wikis(article, extra_wikis=extra_wikis) for article in articles]))
            return True
        else:
            return False


    # events

    async def handle_message(self, message: discord.Message, extra_wikis=None) -> bool:
        if message.author == self.user:
            return False
        if message.author.bot:
            return False
        content = message.content.strip()
        space = self.get_message_space(message)
        if content.startswith(space.command_prefix):
            command_string = content[len(space.command_prefix):].strip()
            await self.process_command(message, space, command_string)
            return True
        elif self.get_in_space(space_id, 'wikitext'):
            return await self.handle_wiki_lookup(message, extra_wikis=extra_wikis)
        else:
            return False

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
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(*args, allowed_mentions=discord.AllowedMentions.none(), intents=intents, chunk_guilds_at_startup=True, **kwargs)
        for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]:
            self.loop.add_signal_handler(sig, lambda sig = sig: asyncio.create_task(self.signal_handler(sig, self.loop)))

        self.default_properties = {
            'command_prefix' : '--',
            'wikitext' : False,
        }

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
            CommandFunction(name='takecommand', value=self.take_command, helpstring='Gain ownership of a simple command'),
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

        self.builtin_command_dict |= OrderedDict([(command.name, command) for command in alias_list])

        self.spaces = {}

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


class Space(abc.ABC):

    custom_command_dict: dict[str, Command] = OrderedDict([])
    crtime = int(time.time())
    mtime = int(time.time())

    def __init__(self, client: DeepBlueSky):
        self.client = client
        self.command_prefix = client.default_properties['command_prefix']
        self.wikitext = client.default_properties['wikitext']

    def load_properties(property_dict: dict[str, Any]):
        if 'command_prefix' in property_dict:
            self.command_prefix = property_dict['command_prefix']
        if 'wikitext' in property_dict:
            self.wikitext = property_dict['wikitext']
        if 'crtime' in property_dict:
            self.crtime = property_dict['crtime']
        if 'mtime' in property_dict:
            self.mtime = property_dict['mtime']

    def load_command(command_dict) -> bool:
        # python 3.10: use patterns
        if command_dict['type'] != 'simple' and command_dict['type'] != 'alias':
            msg = f'Invalid custom command type: {comamnd_dict["type"]}'
            self.client.logger.error(msg)
            raise ValueError(msg)

        author_id = command_dict['author']
        name = command_dict['name']
        crtime = command_dict['crtime']
        mtime = command_dict['mtime']
        value = command_dict['value']

        if command_dict['type'] == 'simple':
            command = CommandSimple(name=name, author=author, creation_time=crtime, modification_time=mtime, value=value)
        else:
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

    def load_commands(command_dict_list) -> bool:
        failed_all = False
        commands_to_add = command_dict_list[:]
        while len(commands_to_add) > 0 and not failed_all:
            failed_all = True
            for command_dict in commands_to_add[:]:
                if self.load_command(command_dict):
                    commands_to_add.remove(command_dict)
                    failed_all = False
        if failed_all:
            self.logger.error(f'Broken aliases detected in space: {space_id}')
        return not failed_all

    @abc.abstractmethod
    async def is_moderator(self, user) -> bool:
        pass

    @abc.abstractmethod
    def get_space_id(self) -> str:
        pass

    @abc.abstractmethod
    def get_space_type(self) -> str:
        pass

    def __str__(self) -> str:
        return self.get_space_id()

class DMSpace(Space):

    recipient_id: int = 0
    recipient: Optional[discord.User] = None

    def __init__(self, client: DeepBlueSky, recipient_id: int):
        super().__init__(client=client)
        self.recipient_id = recipient_id

    async def get_recipient(self) -> discord.User:
        if self.recipient: return self.recipient
        recipient = self.client.get_or_fetch_user(self.recipient_id)
        if not recipient:
            raise RuntimeError(f'Cannot find user: {self.recipient_id}')
        self.recipient = recipient
        return self.recipient

    def get_space_id(self) -> str:
        return f'dm_{self.recipient_id}'

    async def is_moderator(self, user) -> bool:
        return True

    def get_space_type(self) -> str:
        return 'dm'

class ChannelSpace(Space):

    channel_id: int = 0
    channel: Optional[discord.GroupChannel] = None

    def __init__(self, client: DeepBlueSky, channel_id: int, command_list: list[Command] = []):
        super().__init__(client=client, command_list=command_list)
        self.channel_id = channel_id

    async def get_channel(self) -> discord.GroupChannel:
        if self.channel: return self.channel
        channel = self.client.get_or_fetch_channel(self.channel_id)
        if not channel:
            raise RuntimeError(f'Cannot find channel: {channel_id}')
        if channel is not discord.GroupChannel:
            raise RuntimeError(f'Channel is not a GroupChannel: {channel_id}')
        self.channel = channel
        return self.channel

    def get_space_id(self) -> str:
        return f'chan_{self.channel_id}'

    async def is_moderator(self, user) -> bool:
        return True

    def get_space_type(self) -> str:
        return 'chan'

class GuildSpace(Space):

    guild_id: int = 0
    guild: Optional[discord.Guild] = None

    def __init__(self, client: DeepBlueSky, guild_id: int, command_list: list[Command] = []):
        super().__init__(client=client, command_list=command_list)
        self.guild_id = guild_id

    def get_space_id(self) -> str:
        return f'guild_{self.guild_id}'

    def get_guild(self) -> discord.Guild:
        if self.guild: return self.guild
        guild = self.client.get_or_fetch_guild(self.guild_id)
        if not guild:
            raise RuntimeError(f'Cannot find guild: {self.guild_id}')
        self.guild = guild
        return self.guild

    async def is_moderator(self, user) -> bool:
        member = await self.client.get_or_fetch_member(self.get_guild(), user.id)
        return member and member.guild_permissions.kick_members

    def get_space_type(self) -> str:
        return 'guild'

class Command(abc.ABC):
    name: str = ''
    author: Optional[int] = None
    command_type: str = ''
    aliases: list[CommandAlias] = []
    creation_time: Optional[int] = None
    modification_time: Optional[int] = None
    space: Optional[Space] = None

    def __init__(self, name: str, author: Optional[int], command_type: str, creation_time: Optional[int], modification_time: Optional[int]):
        self.name = name
        self.author = author
        self.command_type = command_type
        self.creation_time = creation_time
        self.modification_time = modification_time

    # the returned string is the "result"
    # it is sent to the channel, the caller must not then send it
    @abc.abstractmethod
    async def _invoke0(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        pass

    async def invoke(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        if self.space and self.space != space:
            space.client.logger.error(f'Command {self.name} owned by another space: {self.space}, not {space}')
            return False
        if self.can_call(trigger, space):
            try:
                result = await self._invoke0(trigger, space, name_used, command_predicate)
                if result:
                    space.client.logger.info(f'Command succeeded, author: {trigger.author.id}, name: {self.name}')
                else:
                    space.client.logger.error(f'Command failed, author: {trigger.author.id}, name: {self.name}')
            except Exception ex:
                space.client.logger.critical(f'Unexpected exception during command invocation: {str(ex)}', exc_info=True)
                return False
        else
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

    def canonical(self, path: list = []) -> Command:
        return self

    def follow(self) -> Command:
        return self

    def __eq__(self, other) -> bool:
        if not other:
            return False
        if id(self) == id(other):
            return True
        return self.name == other.name and self.Space == other.Space and self.command_type = other.command_type

    def __hash__(self) -> int:
        return hash((self.name, self.Space, self.command_type))

class CommandSimple(Command):

    value: str = ''
    builtin: bool = False
    helpstring: str = 'a simple command replies with its value'

    def __init__(self, name: str, author: Optional[int] = None,  creation_time: Optional[int] = None, modification_time: Optional[int] = None, value: str, builtin: bool = False, helpstring: Optional[str] = None):
        super().__init__(name=name, author=author, command_type='simple', creation_time=creation_time, modification_time=modification_time)
        self.value = value
        self.builtin = builtin
        if helpstring: self.helpstring = helpstring

    # override
    async def _invoke0(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        try:
            await space.client.send_to_channel(trigger.channel, self.value)
        except discord.Forbidden:
            space.client.logger.error(f'Insufficient permissions to send to channel. id: {trigger.channel.id}, name: {self.name}')
            return False
        return True

    def get_help(self) -> str:
        return self.helpstring

    def is_builtin(self) -> bool:
        return builtin

class CommandAlias(Command):

    builtin: bool = True

    def __init__(self, name: str, author: Optional[int] = None, creation_time: Optional[int] = None, modification_time: Optional[int] = None, value: Command, builtin: Optional[bool] = None):
        super().__init__(name=name, author=author, command_type='alias', creation_time=creation_time, modification_time=creation_time)
        self.value = value
        self.value.aliases += [self]
        self.builtin = builtin if builtin is not None else self.value.is_builtin()

    async def _invoke0(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        return await self.canonical().invoke(trigger, space, name_used, command_predicate)

    def get_help(self) -> str:
        return f'alias for `{self.value.name}`'

    def is_builtin(self) -> bool:
        return builtin

    # Axiom of Regularity
    # https://en.wikipedia.org/wiki/Axiom_of_regularity
    # prevent infinte alias loops
    # This should never fail because alias creation requires another command object
    # but failsafes are good
    def check_regularity(self, path: list = []) -> bool:
        if self in path:
            return False
        if value is CommandAlias not value.verify_regularity(path + [self]): return False
        return True

    def canonical(self, path: list = []) -> Command:
        if self in path:
            raise RuntimeError(f'alias cycle detected: {self.name}, {path}')
        return self.value.canonical(path + [self])

    def follow(self) -> Command:
        return self.value

class CommandFunction(Command):

    def __init__(self, name: str, value: Callable[[discord.Message, Space, str, str], bool], helpstring: str):
        super().__init__(name=name, author=None, command_type='function', creation_time=None, modification_time=None)
        self.value = value
        self.helpstring = helpstring

    def is_builtin(self) -> bool:
        return True

    async def _invoke0(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        return await self.value(self.client, trigger, space, name_used, command_predicate)

    def get_help(self) -> str;
        return self.helpstring
