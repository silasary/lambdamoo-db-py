import enum


class MooTypes(enum.IntEnum):
    INT = 0
    OBJ = 1
    STR = 2
    ERR = 3
    LIST = 4
    CLEAR = 5
    NONE = 6
    _CATCH = 7
    _FINALLY = 8
    FLOAT = 9
    MAP = (10,)
    ANON = (12,)
    WAIF = (13,)
    BOOL = 14


class DBVersions(enum.IntEnum):
    DBV_Prehistory = 0  # Before format versions
    DBV_Exceptions = (
        1  # Addition of the `try', `except', `finally', and `endtry' keywords.
    )
    DBV_BreakCont = 2  # Addition of the `break' and `continue' keywords.
    DBV_Float = 3  # Addition of `FLOAT' and `INT' variables and the `E_FLOAT' keyword, along with version numbers on each frame of a suspended task.
    DBV_BFBugFixed = 4  # Bug in built-in function overrides fixed by making it use tail-calling.  This DB_Version change exists solely to turn off special bug handling in read_bi_func_data().
    DBV_NextGen = 5  # Introduced the next-generation database format which fixes the data locality problems in the v4 format.
    DBV_TaskLocal = 6  # Addition of task local value.
    DBV_Map = 7  #  Addition of `MAP' variables
    DBV_FileIO = 8  #  Includes addition of the 'E_FILE' keyword.
    DBV_Exec = 9  #  Includes addition of the 'E_EXEC' keyword.
    DBV_Interrupt = 10  #  Includes addition of the 'E_INTRPT' keyword.
    DBV_This = 11  #  Varification of `this'.
    DBV_Iter = 12  #  Addition of map iterator
    DBV_Anon = 13  #  Addition of anonymous objects
    DBV_Waif = 14  #  Addition of waifs
    DBV_Last_Move = 15  #  Addition of the 'last_move' built-in property
    DBV_Threaded = 16  #  Store threading information
    DBV_Bool = 17  #  Boolean type
    Num_DB_Versions = 18  #  Special: the current version is this - 1.
