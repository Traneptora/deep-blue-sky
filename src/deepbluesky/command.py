# command.py
from __future__ import annotations
import abc

from typing import TYPE_CHECKING, Any, Optional
from typing import Awaitable, Callable, Dict, List

import discord

if TYPE_CHECKING:
    from .space import Space

class Command(abc.ABC):

    # pylint: disable=function-redefined

    def __init__(self, name: str, author: Optional[int], command_type: str, creation_time: Optional[int], modification_time: Optional[int], space: Optional[Space] = None):
        self.name = name
        self.author = author
        self.aliases: List[CommandAlias] = []
        self.command_type = command_type
        self.creation_time = creation_time
        self.modification_time = modification_time
        self.space = space

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
                return result
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

    def __init__(self, name: str, value: str, author: Optional[int] = None,  creation_time: Optional[int] = None, modification_time: Optional[int] = None, space: Optional[Space] = None, builtin: bool = False, helpstring: Optional[str] = None):
        super().__init__(name=name, author=author, command_type='simple', creation_time=creation_time, modification_time=modification_time, space=space)
        self.value = value
        self.builtin = builtin
        self.helpstring = helpstring if helpstring else 'a simple command replies with its value'

    # override
    async def _invoke0(self, trigger: discord.Message, space: Space, name_used: str, command_predicate: Optional[str]) -> bool:
        try:
            await space.client.send_to_channel(trigger.channel, trigger.reference if trigger.reference else trigger, self.value)
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

    def __init__(self, name: str, value: Command, author: Optional[int] = None, creation_time: Optional[int] = None, modification_time: Optional[int] = None, space: Optional[Space] = None, builtin: Optional[bool] = None):
        super().__init__(name=name, author=author, command_type='alias', creation_time=creation_time, modification_time=creation_time, space=space)
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
        return not isinstance(self.value, CommandAlias) or self.value.check_regularity(path + [self])

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

    def __init__(self, name: str, value: Callable[[discord.Message, Space, str, Optional[str]], Awaitable[bool]], helpstring: str):
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
