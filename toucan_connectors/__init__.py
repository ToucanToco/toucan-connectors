from contextlib import suppress

with suppress(ImportError):
    from .mongo import *
with suppress(ImportError):
    from .mssql import *
with suppress(ImportError):
    from .mysql import *
with suppress(ImportError):
    from .postgres import *
