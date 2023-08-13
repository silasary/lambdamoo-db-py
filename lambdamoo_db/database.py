from typing import Any, Generator
import attrs
from .enums import MooTypes, ObjectFlags, PropertyFlags


class ObjNum(int):
    pass


class Anon(int):
    pass


@attrs.define()
class Verb:
    name: str
    owner: int
    perms: int
    preps: int
    object: int
    code: list[str] = attrs.field(init=False, factory=list)


@attrs.define()
class Property:
    propertyName: str
    value: Any
    owner: int
    perms: PropertyFlags = attrs.field(converter=PropertyFlags)


@attrs.define()
class MooObject:
    id: int
    name: str
    flags: ObjectFlags = attrs.field(converter=ObjectFlags)
    owner: int
    location: int
    parents: list[int] = attrs.field(factory=list)
    children: list[int] = attrs.field(init=False, factory=list)
    last_move: int = attrs.field(init=False, default=-1)
    contents: list[int] = attrs.field(init=False, factory=list)
    verbs: list[Verb] = attrs.field(init=False, factory=list)
    properties: list[Property] = attrs.field(init=False, factory=list)
    anon: bool = attrs.field(default=False)

    @property
    def parent(self) -> int:
        if len(self.parents) > 1:
            raise Exception("Object has multiple parents")
        return self.parents[0]


@attrs.define()
class Waif:
    waif_class: int
    owner: int
    props: list[Any]


@attrs.define()
class WaifReference:
    index: int


@attrs.define()
class Activation:
    this: int | None = attrs.field(init=False, default=None)
    threaded: int | None = attrs.field(init=False, default=None)
    player: int | None = attrs.field(init=False, default=None)
    programmer: int | None = attrs.field(init=False, default=None)
    vloc: int | None = attrs.field(init=False, default=None)
    debug: bool = attrs.field(init=False)
    verb: str = attrs.field(init=False)
    verbname: str = attrs.field(init=False)
    code: list[str] = attrs.field(init=False, factory=list)
    stack: list[Any] = attrs.field(init=False, factory=list)
    unused1 = attrs.field(init=False, default=0)
    unused2 = attrs.field(init=False, default=0)
    unused3 = attrs.field(init=False, default=0)
    unused4 = attrs.field(init=False, default=0)


@attrs.define()
class VM:
    locals: dict
    stack: list[Activation | None]


@attrs.define()
class QueuedTask:
    firstLineno: int
    id: int
    st: int
    unused: int = attrs.field(init=False, default=0)
    value: Any = attrs.field(init=False, default=None)
    activation: Activation | None = attrs.field(init=False)
    rtEnv: dict[str, Any] = attrs.field(init=False)
    code: list[str] = attrs.field(init=False, factory=list)


@attrs.define()
class SuspendedTask:
    firstLineno: int
    id: int
    st: int
    value: Any = attrs.field(init=False, default=None)
    vm: VM = attrs.field(init=False, default=None)

@attrs.define()
class InterruptedTask:
    id: int
    status: str
    vm: VM = attrs.field(default=None)

@attrs.define()
class Connection:
    who: int
    listener: int

TYPE_MAPPING = {
    int: MooTypes.INT,
    str: MooTypes.STR,
    ObjNum: MooTypes.OBJ,
    float: MooTypes.FLOAT,
    list: MooTypes.LIST,
    dict: MooTypes.MAP,
    bool: MooTypes.BOOL,
    type(None): MooTypes.NONE,
}


@attrs.define
class MooDatabase:
    versionstring: str = attrs.field(init=False)
    version: int = attrs.field(init=False)
    total_objects: int = attrs.field(init=False, default=0)
    total_verbs: int = attrs.field(init=False, default=0)
    total_players: int = attrs.field(init=False, default=0)
    clocks: list = attrs.field(factory=list)
    objects: dict[int, MooObject] = attrs.field(factory=dict)
    queuedTasks: list[QueuedTask] = attrs.field(factory=list)
    suspendedTasks: list[SuspendedTask] = attrs.field(factory=list)
    interruptedTasks: list[InterruptedTask] = attrs.field(factory=list)
    connections: list[Connection] = attrs.field(factory=list)
    waifs: dict[int, Waif] = attrs.field(factory=dict)
    players: list[int] = attrs.field(factory=list)
    def all_verbs(self) -> Generator[Verb, None, None]:
        for obj in self.objects.values():
            for verb in obj.verbs:
                yield verb
