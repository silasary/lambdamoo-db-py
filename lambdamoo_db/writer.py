from io import TextIOWrapper
from typing import Any
from attrs import define, asdict

from lambdamoo_db.enums import MooTypes

from . import templates
from .database import TYPE_MAPPING, VM, Activation, MooDatabase, MooObject, ObjNum, Propdef, QueuedTask, SuspendedTask, InterruptedTask, Verb, _Catch, Clear, Err, Anon


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
        self.write(f"{f:.19g}\n")

    def writeBool(self, b: bool) -> None:
        return self.writeInt(1 if b else 0)

    def writeList(self, l: list[Any]) -> None:
        return self.writeCollection(l, writer=self.writeValue)

    def writeMap(self, m: dict[str, Any]) -> None:
        def writeMapItem(item):
            key, value = item
            self.writeValue(key)
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
        elif value_type == type(None):
            self.writeInt(MooTypes.NONE.value)
        elif value_type == _Catch:
            self.writeInt(MooTypes._CATCH.value)
            self.writeInt(v)
        elif value_type == Clear:
            self.writeInt(MooTypes.CLEAR.value)
        elif value_type == Err:
            self.writeInt(MooTypes.ERR.value)
            self.writeInt(v)
        elif value_type == Anon:
            self.writeInt(MooTypes.ANON.value)
            self.writeInt(v)
        else:
            raise Exception(f"Unknown type {value_type}")   

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
        self.writeAnons()
        self.writeInt(0)
        self.writeVerbs()

    def writePlayers(self) -> None:
        self.writeCollection(self.db.players, writer=self.writeString)

    def writePending(self) -> None:
        self.writeCollection(self.db.finalizations, template=templates.pending_values_count, writer=self.writeFinalization)

    def writeFinalization(self, v):
        self.writeValue(v)

    def writeObjects(self) -> None:
        objects = [o for o in self.db.objects.values() if not o.anon]
        self.writeCollection(objects, writer=self.writeObject)

    def writeAnons(self) -> None:
        objects = [o for o in self.db.objects.values() if o.anon]
        self.writeCollection(objects, writer=self.writeObject)

    def writeObject(self, obj: MooObject) -> None:
        obj_num = obj.id
        if obj.recycled:
            self.writeString(f"# {obj_num} recycled")
            return
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
        self.writeCollection(obj.propnames, None, lambda prop: self.writeString(prop))
        self.writeCollection(obj.propdefs, None, self.write_propdef)

    def write_propdef(self, prop: Propdef):
        self.writeValue(prop.value)
        self.writeInt(prop.owner)
        self.writeInt(prop.perms.value)

    def writeVerbs(self) -> None:
        verbs = [verb for verb in self.db.all_verbs() if verb.code is not None]
        self.writeInt(len(verbs))
        for verb in verbs:
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

    def writeTaskQueue(self):
        self.writeCollection(self.db.queuedTasks, templates.task_count, self.writeQueuedTask)

    def writeQueuedTask(self, task: QueuedTask) -> str:
        taskHeader = templates.task_header.format(**asdict(task))
        self.writeString(taskHeader)
        self.writeActivationAsPI(task.activation)
        self.writeRtEnv(task.rtEnv)
        self.writeCode(task.code)

    def writeActivationAsPI(self, activation: Activation):
        self.writeValue(-111)
        self.writeValue(activation.this)
        self.writeValue(activation.vloc)
        self.writeInt(activation.threaded)
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
        self.writeCode(activation.code)
        self.writeRtEnv(activation.rtEnv)
        header = templates.stack_header.format(slots=len(activation.stack))
        self.writeString(header)
        for i in activation.stack:
            self.writeValue(i)
        self.writeActivationAsPI(activation)
        self.writeValue(activation.temp)
        header = templates.pc.format(**asdict(activation))
        self.writeString(header)
        if activation.bi_func:
            self.writeString(activation.func_name)

    def writeSuspendedTasks(self):
        self.writeCollection(self.db.suspendedTasks, templates.suspended_task_count, self.writeSuspendedTask)

    def writeSuspendedTask(self, task: SuspendedTask):
        header = templates.suspended_task_header.format(**asdict(task))
        self.writeString(header)
        self.writeVM(task.vm)

    def writeVM(self, vm: VM):
        self.writeValue(vm.locals)
        header = templates.vm_header.format(**asdict(vm))
        self.writeString(header)
        for i in range(vm.top + 1):
            self.writeActivation(vm.stack[i])

    def writeRtEnv(self, env: dict[str, Any]):
        header = templates.var_count.format(count=len(env))
        self.writeString(header)
        for name, value in env.items():
            self.writeString(name)
            self.writeValue(value)


    def writeInterruptedTasks(self):
        self.writeCollection(self.db.interruptedTasks, templates.interrupted_task_count, self.writeInterruptedTask)

    def writeInterruptedTask(self, task: InterruptedTask):
        header = templates.interrupted_task_header.format(**asdict(task))
        self.writeString(header)
        self.writeVM(task.vm)

    def writeConnections(self):
        # these are not useful
        self.writeCollection(self.db.connections, "{count} active connections with listeners", self.writeConnection)

    def writeConnection(self, connection):
        self.writeString(f"{connection.who} {connection.listener}")

def dump(db: MooDatabase, f: TextIOWrapper) -> None:
    writer = Writer(db=db, f=f)
    writer.writeDatabase()
