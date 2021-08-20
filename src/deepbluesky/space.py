# space.py
from __future__ import annotations
import abc
import json
import os
import re
import time

from typing import TYPE_CHECKING, Any, Optional
from typing import Dict, FrozenSet, List, OrderedDict

import discord
from .command import Command, CommandAlias, CommandSimple

if TYPE_CHECKING:
    from .deepbluesky import DeepBlueSky

class Space(abc.ABC):

    # pylint: disable=function-redefined

    def __init__(self, client: DeepBlueSky, space_type: str, base_id: int):
        self.client: DeepBlueSky = client
        self.base_id: int = base_id
        self.custom_command_dict: Dict[str, Command] = OrderedDict([])
        self.crtime: int = int(time.time())
        self.mtime: int = int(time.time())
        self.wikitext: Optional[bool] = None
        self.command_prefix: Optional[str] = None
        self.space_type: str = space_type
        self.space_id: str = f'{space_type}_{base_id}'

    def __str__(self) -> str:
        return self.space_id

    def __eq__(self, other) -> bool:
        if not other:
            return False
        if id(self) == id(other):
            return True
        return self.space_id == other.space_id

    def __hash__(self) -> int:
        return hash((type(self), self.space_id))

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
            self.custom_command_dict[name] = CommandSimple(name=name, author=author, creation_time=crtime, modification_time=mtime, value=value)
        else:
            # command_type must equal 'alias'
            if value in self.client.builtin_command_dict:
                value = self.client.builtin_command_dict[value]
            elif value in self.custom_command_dict:
                value = self.custom_command_dict[value]
            else:
                self.client.logger.warning(f'cant add alias before its target. name: {name}, value: {value}')
                return False
            self.custom_command_dict[name] = CommandAlias(name=name, author=author, creation_time=crtime, modification_time=mtime, value=value, builtin=False)
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
        user_id = -1
        userlist = await self.get_userlist()
        query = query.lower()
        for user in frozenset({self.client.user}).union(userlist):
            fullname = user.name.lower() + '#' + user.discriminator
            displayname = user.display_name.lower()
            if fullname.startswith(query) or displayname.startswith(query):
                if user_id >= 0:
                    return -2
                user_id = user.id
        return user_id

    @abc.abstractmethod
    async def get_userlist(self) -> FrozenSet[discord.abc.User]:
        pass

    @abc.abstractmethod
    def is_moderator(self, user: discord.abc.User) -> bool:
        pass

class DMSpace(Space):

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

    def is_moderator(self, user: discord.abc.User) -> bool:
        return True

    async def get_userlist(self) -> FrozenSet[discord.abc.User]:
        return frozenset({await self.get_recipient()})

class ChannelSpace(Space):

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
        if not isinstance(channel, discord.GroupChannel):
            msg = f'Channel is not a GroupChannel: {self.base_id}'
            self.client.logger.critical(msg)
            raise RuntimeError(msg)
        self.channel = channel
        return self.channel

    def is_moderator(self, user: discord.abc.User) -> bool:
        return True

    async def get_userlist(self) -> FrozenSet[discord.abc.User]:
        return frozenset((await self.get_channel()).recipients)

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

    async def get_userlist(self) -> FrozenSet[discord.abc.User]:
        return frozenset((await self.get_guild()).members)
