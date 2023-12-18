from typing import Any, Generator

import attrs

from .enums import MooTypes, ObjectFlags, PropertyFlags


class ObjNum(int):
    pass


class Anon(int):
    pass


class _Catch(int):
    pass


class Clear:
    pass


class Err(int):
    pass


@attrs.define()
class Verb:
    name: str
    owner: int
    perms: int
    preps: int
    object: int
    code: list[str] | None = None


@attrs.define()
class Propdef:
    value: Any
    owner: int
    perms: PropertyFlags = attrs.field(converter=PropertyFlags)


@attrs.define()
class MooObject:
    db: "MooDatabase" = attrs.field()
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
    propnames: list[str] = attrs.field(init=False, factory=list)
    propdefs: list[Propdef] = attrs.field(init=False, factory=list)
    anon: bool = attrs.field(default=False)
    recycled: bool = attrs.field(default=False)

    @property
    def parent(self) -> int:
        if len(self.parents) > 1:
            raise Exception("Object has multiple parents")
        return self.parents[0]

    def prop_index(self, name: str) -> int:
        try:
            return self.propnames.index(name)
        except ValueError:
            return -1


@attrs.define()
class Waif:
    waif_class: int
    owner: int
    prop_indexes: list[Any]
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
    rtEnv: dict[str, Any] = attrs.field(init=False, factory=dict)
    temp: Any = attrs.field(init=False, default=None)
    pc: int = attrs.field(init=False, default=0)
    bi_func: int = attrs.field(init=False, default=0)
    func_name: str = attrs.field(init=False, default="")
    error: int = attrs.field(init=False, default=0)


@attrs.define()
class VM:
    locals: dict
    stack: list[Activation | None]
    top: int
    vector: int
    funcId: int
    maxStackframes: int


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
    firstLineno: int = attrs.field(default=0)
    id: int = attrs.field(default=0)
    start_time: int = attrs.field(default=0)
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
    finalizations: list = attrs.field(factory=list)
    clocks: list = attrs.field(factory=list)
    objects: dict[int, MooObject] = attrs.field(factory=dict)
    queuedTasks: list[QueuedTask] = attrs.field(factory=list)
    suspendedTasks: list[SuspendedTask] = attrs.field(factory=list)
    interruptedTasks: list[InterruptedTask] = attrs.field(factory=list)
    connections: list[Connection] = attrs.field(factory=list)
    waifs: dict[int, Waif] = attrs.field(factory=dict)
    players: list[int] = attrs.field(factory=list)

    def ancestors(self, obj: MooObject) -> Generator[MooObject, None, None]:
        yield obj
        for parent in obj.parents:
            if parent == -1:
                continue
            yield from self.ancestors(self.objects[parent])

    def all_verbs(self) -> Generator[Verb, None, None]:
        for obj in self.objects.values():
            for verb in obj.verbs:
                yield verb

    def set_property(self, obj_id: int, prop_name: str, value: Any) -> None:
        obj = self.objects.get(obj_id)
        if obj is None:
            raise ValueError(f"Object {obj_id} not found")

        prop_index = self._find_property_index(obj, prop_name)
        if prop_index is None:
            raise ValueError(f"Property {prop_name} not defined on object {obj_id} or its ancestors")

        # Set clear property
        if isinstance(value, Clear):
            obj.propdefs[prop_index].value = Clear
        else:
            # Set non-clear property
            obj.propdefs[prop_index].value = value
            # Handle inheritance: update value in descendants if they have clear properties
            self._update_descendants_clear_properties(obj_id, prop_name, value)

    def _find_property_index(self, obj: MooObject, prop_name: str) -> int | None:
        """
        Recursively search for a property in the object and its ancestors.
        Returns the index of the property or None if not found.
        """
        if prop_name in obj.propnames:
            return obj.propnames.index(prop_name)

        for parent_id in obj.parents:
            parent = self.objects.get(parent_id)
            if parent:
                parent_index = self._find_property_index(parent, prop_name)
                if parent_index is not None:
                    return parent_index
        return None

    def _update_descendants_clear_properties(self, obj_id: int, prop_name: str, value: Any) -> None:
        for child_id in self.objects[obj_id].children:
            child = self.objects[child_id]
            prop_index = self._find_property_index(child, prop_name)
            if prop_index is not None and isinstance(child.propdefs[prop_index].value, Clear):
                child.propdefs[prop_index].value = value
                self._update_descendants_clear_properties(child_id, prop_name, value)

    def get_property(self, obj_id: int, prop_name: str) -> Any:
        obj = self.objects.get(obj_id)
        if obj is None:
            raise ValueError(f"Object {obj_id} not found")

        prop_index = self._find_property_index(obj, prop_name)
        if prop_index is None:
            raise ValueError(f"Property {prop_name} not defined on object {obj_id} or its ancestors")

        return self._resolve_property_value(obj, prop_name, prop_index)

    def _resolve_property_value(self, obj: MooObject, prop_name: str, prop_index: int) -> Any:
        value = obj.propdefs[prop_index].value
        if not isinstance(value, Clear):
            return value

        # Property is clear, resolve from ancestors
        for parent_id in obj.parents:
            parent = self.objects.get(parent_id)
            if parent:
                parent_prop_index = self._find_property_index(parent, prop_name)
                if parent_prop_index is not None:
                    return self._resolve_property_value(parent, prop_name, parent_prop_index)

        return Clear  # Return Clear if no non-clear value found in ancestors

    def rename_property(self, obj_id: int, old_name: str, new_name: str) -> None:
        obj = self.objects.get(obj_id)
        if obj is None:
            raise ValueError(f"Object {obj_id} not found")

        prop_index = self._find_property_index(obj, old_name)
        if prop_index is None:
            raise ValueError(f"Property {old_name} not found on object {obj_id}")

        # Check for name conflicts
        if self._find_property_index(obj, new_name) is not None:
            raise ValueError(f"Property {new_name} already exists on object {obj_id}")

        # Rename the property
        obj.propnames[prop_index] = new_name
        self._update_descendants_property_name(obj_id, old_name, new_name)

    def _update_descendants_property_name(self, obj_id: int, old_name: str, new_name: str) -> None:
        for child_id in self.objects[obj_id].children:
            child = self.objects[child_id]
            prop_index = self._find_property_index(child, old_name)
            if prop_index is not None:
                child.propnames[prop_index] = new_name
                self._update_descendants_property_name(child_id, old_name, new_name)

    def get_property_dict(self, obj_id: int) -> dict[str, Any]:
        obj = self.objects.get(obj_id)
        if obj is None:
            raise ValueError(f"Object {obj_id} not found")

        prop_dict = {}
        if obj.parent != -1:
            prop_dict = self.get_property_dict(obj.parent)
        for prop_name in obj.propnames:
            prop_dict[prop_name] = self.get_property(obj_id, prop_name)

        return prop_dict
