from io import TextIOWrapper
from typing import Any
from attrs import define
from .database import MooDatabase, MooObject, ObjNum, Property, Verb


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
        self.writeInt(len(l))
        self.write("\n")
        for v in l:
            self.writeValue(v)
            self.write("\n")

    def writeMap(self, m: dict[str, Any]) -> None:
        self.writeInt(len(m))
        self.write("\n")
        for k, v in m.items():
            self.writeString(k)
            self.writeValue(v)
            self.write("\n")

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
        version_str = "** LambdaMOO Database, Format Version 17 **"
        self.writeString(version_str)
        self.writePlayers()
        self.writePending()

    def writePlayers(self) -> None:
        self.writeInt(self.db.total_players)
        self.write("\n")
        for p in self.db.players:
            self.writeInt(p)
            self.write("\n")

    def writePending(self) -> None:
        pass

    def writeObjects(self) -> None:
       for obj_num, obj  in self.db.objects.items():
        self.writeObject(obj_num, obj)

    def writeObject(self, obj_num: int, obj: MooObject) -> None:
        self.writeString(f"#{obj_num}")
        self.writeString(obj.name)
        self.writeInt(obj.flags)
        self.writeInt(obj.owner)
        self.writeValue(obj.location)
        self.writeValue(obj.last_move)
        self.writeValue(obj.contents)
        self.writeValue(obj.parents)
        self.writeValue(obj.children)
        self.writeInt(len(obj.verbs))
        self.write("\n")
        for verb in obj.verbs:
            self.writeVerbMetadata(verb)

    def writeVerbMetadata(self, verb: Verb) -> None:
        self.writeString(verb.name)
        self.writeInt(verb.owner)
        self.writeInt(verb.perms)
        self.writeInt(verb.preps)
        self.write("\n")

    def write_properties(self, obj: MooObject) -> None:
        self.writeInt(len(obj.properties))
        self.write("\n")
        for prop in obj.properties:
            self.writeString(prop.propertyName)
        self.writeInt(len(obj.properties))
        self.write("\n")
        for prop in obj.properties:
            self.writeValue(prop.value)
            self.writeInt(prop.owner)
            self.writeInt(prop.perms)
