from io import TextIOWrapper
from typing import Any
from attrs import define, asdict

from lambdamoo_db.enums import MooTypes

from . import templates
from .database import TYPE_MAPPING, VM, Activation, MooDatabase, MooObject, ObjNum, Property, QueuedTask, SuspendedTask, Verb


@define
class Writer:
    db: MooDatabase
    f: TextIOWrapper

    def write(self, text) -> None:
        self.f.write(text)

    def writeInt(self, i: int) -> None:
        self.write(f"{i:d}\n")

    def writeString(self, s: str) -> None:
        self.write(f"{s}\n")

    def writeObj(self, obj: ObjNum) -> None:
        self.write(f"{obj:d}\n")

    def writeFloat(self, f: float) -> None:
        self.write(f"{f: f}\n")

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
            self.writeInt(MooTypes.INT.value)
            self.writeInt(v)
        elif value_type == str:
            self.writeInt(MooTypes.STR.value)
            self.writeString(v)
        elif value_type == ObjNum:
            self.writeInt(MooTypes.OBJ.value)
            self.writeObj(v)
        elif value_type == float:
            self.writeInt(MooTypes.FLOAT.value)
            self.writeFloat(v)
        elif value_type == bool:
            self.writeInt(MooTypes.BOOL.value)
            self.writeBool(v)
        elif value_type == list:
            self.writeInt(MooTypes.LIST.value)
            self.writeList(v)
        elif value_type == dict:
            self.writeInt(MooTypes.MAP.value)
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
        # self.writeInterruptedTasks()
        self.writeConnections()
        self.writeObjects()
        self.writeVerbs()

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
        self.writeInt(obj.flags.value)
        self.writeInt(obj.owner)
        self.writeValue(obj.location)
        self.writeValue(obj.last_move)
        self.writeValue(obj.contents)
        if len(obj.parents) == 1:
            self.writeValue(obj.parents[0])
        else:
            self.writeValue(obj.parents)
        self.writeValue(obj.children)
        self.writeCollection(obj.verbs, writer=self.writeVerbMetadata)
        self.write_properties(obj)

    def writeVerbMetadata(self, verb: Verb) -> None:
        self.writeString(verb.name)
        self.writeInt(verb.owner)
        self.writeInt(verb.perms)
        self.writeInt(verb.preps)

    def write_properties(self, obj: MooObject) -> None:
        self.writeCollection(obj.properties, None, lambda prop: self.writeString(prop.propertyName))
        self.writeCollection(obj.properties, None, self.writeProperty)

    def writeProperty(self, prop: Property):
        self.writeValue(prop.value)
        self.writeInt(prop.owner)
        self.writeInt(prop.perms)

    def writeVerbs(self) -> None:
        for verb in self.db.all_verbs():
            self.writeVerb(verb)

    def writeVerb(self, verb: Verb) -> None:
        objnum = verb.object
        object = self.db.objects[objnum]
        index = object.verbs.index(verb)
        vloc = f"#{objnum}:{index}"
        self.writeString(vloc)
        self.writeCode(verb.code)

    def writeCode(self, code: list[str]) -> None:
        for line in code:
            self.writeString(line)
        self.writeString(".")

    def writeCollection(self, collection, template=None, writer=None):
        if writer is None:
            writer = self.writeString
        if template is None:
            self.writeInt(len(collection))
        else:
            self.writeString(template.format(count=len(collection)))
        for item in collection:
            writer(item)

    def writeClocks(self):
        self.writeCollection(self.db.clocks, templates.clock_count)

    def writeSuspendedTasks(self):
        self.writeCollection(self.db.suspendedTasks, templates.suspended_task_count, self.writeSuspendedTask)

    def writeSuspendedTask(self, task: SuspendedTask):
        task_header = templates.suspended_task_header.format(**asdict(task))
        self.writeString(task_header)

    def writeTaskQueue(self):
        self.writeCollection(self.db.queuedTasks, templates.task_count, self.writeQueuedTask)

    def writeQueuedTask(self, task: QueuedTask) -> str:
        taskHeader = templates.task_header.format(**asdict(task))
        self.writeString(taskHeader)
        self.writeActivation(task.activation)
        self.writeRtEnv(task.rtEnv)
        self.writeCode(task.code)

    def writeActivationAsPI(self, activation: Activation):
        self.writeValue(activation.unused1)
        self.writeValue(activation.this)
        self.writeValue(activation.unused1)
        self.writeValue(activation.threaded)
        self.writeValue(activation.vloc)
        self.write("\n")
        activation_header = templates.activation_header.format(**asdict(activation))
        self.writeString(activation_header)
        self.writeString("No")
        self.writeString("More")
        self.writeString("Parse")
        self.writeString("Infos")
        self.writeString(activation.verb)
        self.writeString(activation.verbname)

    def writeActivation(self, activation):
        langver = templates.langver.format(version=17)
        self.writeString(langver)
        self.writeActivationAsPI(activation)

    def writeSuspendedTasks(self):
        self.writeCollection(self.db.suspendedTasks, templates.task_count, self.writeSuspendedTask)

    def writeSuspendedTask(self, task: SuspendedTask):
        header = templates.suspended_task_header.format(asdict(task))
        self.writeString(header)
        self.writeVM(task.vm)

    def writeVM(self, vm: VM):
        self.writeValue(vm.locals)

    def writeRtEnv(self, env: dict[str, Any]):
        header = templates.var_count.format(count=len(env))
        self.writeString(header)
        for name, value in env.items():
            self.writeString(name)
            moo_type = TYPE_MAPPING[type(value)]
            self.writeInt(moo_type)
            if (moo_type != MooTypes.NONE):
                self.write("\n")
                self.writeValue(value)
            self.write("\n")


    def writeConnections(self):
        # these are not useful
        self.writeCollection([], "{count} active connections")

def dump(db: MooDatabase, f: TextIOWrapper) -> None:
    writer = Writer(db=db, f=f)
    writer.writeDatabase()
