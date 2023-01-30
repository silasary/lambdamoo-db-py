from io import TextIOWrapper
from typing import Any
from attrs import define, asdict

from . import templates
from .database import VM, MooDatabase, MooObject, ObjNum, Property, SuspendedTask, Verb


@define
class Writer:
    db: MooDatabase
    f: TextIOWrapper

    def write(self, text) -> None:
        self.f.write(text)

    def writeInt(self, i: int) -> None:
        self.write(f"{i: d}")

    def writeString(self, s: str) -> None:
        self.write(f"{s: s}\n")

    def writeObj(self, obj: ObjNum) -> None:
        self.write(f"{obj: d}")

    def writeFloat(self, f: float) -> None:
        self.write(f"{f: f}")

    def writeBool(self, b: bool) -> None:
        return self.writeInt(1 if b else 0)

    def writeList(self, l: list[Any]) -> None:
        return self.writeCollection(l, writer=self.writeValue)

    def writeMap(self, m: dict[str, Any]) -> None:
        def writeMapItem(item):
            key, value = item
            self.writeString(key)
            self.writeValue(value)
        return self.writeCollection(m.items(), writer=writeMapItem)

    def writeValue(self, v: Any) -> None:
        value_type = type(v)
        if value_type == int:
            self.writeInt(v)
        elif value_type == str:
            self.writeString(v)
        elif value_type == ObjNum:
            self.writeObj(v)
        elif value_type == float:
            self.writeFloat(v)
        elif value_type == bool:
            self.writeBool(v)
        elif value_type == list:
            self.writeList(v)
        elif value_type == dict:
            self.writeMap(v)
        else:
            raise Exception("Unknown value type")

    def writeDatabase(self) -> None:
        self.writeString(templates.version.format(version=17))
        self.writePlayers()
        self.writePending()
        self.writeClocks()
        self.writeTaskQueue()
        self.writeSuspendedTasks()
        self.writeInterruptedTasks()
        self.writeConnections()
        self.writeObjects()

    def writePlayers(self) -> None:
        self.writeCollection(self.db.players, writer=self.writeString)

    def writePending(self) -> None:
        pass

    def writeObjects(self) -> None:
        self.writeCollection(self.db.objects.values(), writer=self.writeObject)

    def writeObject(self, obj: MooObject) -> None:
        obj_num = obj.id
        self.writeString(f"#{obj_num}")
        self.writeString(obj.name)
        self.writeInt(obj.flags)
        self.writeInt(obj.owner)
        self.writeValue(obj.location)
        self.writeValue(obj.last_move)
        self.writeValue(obj.contents)
        self.writeValue(obj.parents)
        self.writeValue(obj.children)
        self.write("\n")
        self.writeCollection(obj.verbs, writer=self.writeVerbMetadata)

    def writeVerbMetadata(self, verb: Verb) -> None:
        self.writeString(verb.name)
        self.writeInt(verb.owner)
        self.writeInt(verb.perms)
        self.writeInt(verb.preps)
        self.write("\n")

    def write_properties(self, obj: MooObject) -> None:
        self.writeCollection(obj.properties, None, lambda prop: self.writeString(prop.propertyName))
        self.writeCollection(obj.properties, None, self.writeProperty)

    def writeProperty(self, prop):
        self.writeValue(prop.value)
        self.writeInt(prop.owner)
        self.writeInt(prop.perms)

    def writeVerbs  (self) -> None:
        for verb in self.db.all_verbs():
            self.writeVerb(verb)

    def writeVerb(self, verb: Verb) -> None:
        objnum = verb.object
        object = self.db.objects[objnum]
        index = object.verbs.index(verb)
        vloc = f"{objnum}:{index}"
        self.writeString(vloc)
        self.writeCode(verb.code)

    def writeCode(self, code: list) -> None:
        for line in code:
            self.writeString(line)
        self.writeString(".")

    def writeCollection(self, collection, template=None, writer=None):
        if writer is None:
            writer = self.writeString
        if template is None:
            self.writeInt(len(collection))
            self.write("\n")
        else:
            self.writeString(template.format(count=len(collection)))
        for item in collection:
            writer(item)

    def writeClocks(self):
        self.writeCollection(self.db.clocks, templates.clock_count)

    def writeTaskQueue(self):
        self.writeCollection(self.db.queuedTasks, templates.task_count, self.writeQueuedTask)

    def writeQueuedTask(self, task):
        self.writeInt(task.time)
        self.writeString(task.verb)
        self.writeValue(task.object)
        self.writeValue(task.player)
        self.writeValue(task.arglist)

    def writeSuspendedTasks(self):
        self.writeCollection(self.db.suspendedTasks, templates.task_count, self.writeSuspendedTask)

    def writeSuspendedTask(self, task: SuspendedTask):
        header = templates.suspended_task_header.format(asdict(task))
        self.writeString(header)
        self.writeVM(task.vm)

    def writeVM(self, vm: VM):
        self.writeValue(vm.locals)
