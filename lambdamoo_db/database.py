from typing import Any
import attrs


@attrs.define()
class Verb:
    name: str
    owner: int
    perms: int
    preps: int

    code: list[str] = attrs.field(init=False, factory=list)


@attrs.define()
class Property:
    propertyName: str
    value: Any
    owner: int
    perms: int


@attrs.define()
class MooObject:
    id: int
    name: str
    flags: int
    owner: int
    location: int
    parent: int

    verbs: list[Verb] = attrs.field(init=False, factory=list)
    properties: list[Property] = attrs.field(init=False, factory=list)


@attrs.define(init=False)
class Activation:
    this: int
    player: int
    programmer: int
    vloc: int
    debug: bool
    verb: str
    verbname: str


@attrs.define()
class QueuedTask:
    firstLineno: int
    id: int
    st: int

    activation: Activation = attrs.field(init=False)
    rtEnv: dict[str, Any] = attrs.field(init=False)
    code: list[str] = attrs.field(init=False, factory=list)


@attrs.define(init=False)
class MooDatabase:
    versionstring: str
    version: int

    total_objects: int
    total_verbs: int
    total_players: int

    players: list[int]
    objects: dict[int, MooObject]
    queuedTasks: list[QueuedTask]
