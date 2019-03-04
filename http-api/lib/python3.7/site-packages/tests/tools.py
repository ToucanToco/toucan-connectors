import io
import tempfile
import zipfile

import joblib
import pandas as pd


DF = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
DF2 = pd.DataFrame({'a': ['a', 'b'], 'b': ['c', 'd']})


def default_zip_file(df, df2):
    # type: (pd.DataFrame, pd.DataFrame) -> bytes
    """Return zip file with two DF saved using joblib."""
    with io.BytesIO() as memory_file:
        with zipfile.ZipFile(memory_file, mode='w') as zfile:
            tmp = tempfile.NamedTemporaryFile()
            with open(tmp.name, mode='wb'):
                joblib.dump(df, tmp.name)
            with open(tmp.name, mode='rb') as f:
                content = f.read()
                zfile.writestr('df', content)
            tmp.close()

            tmp2 = tempfile.NamedTemporaryFile()
            with open(tmp2.name, mode='wb'):
                joblib.dump(df2, tmp2.name)
            with open(tmp2.name, mode='rb') as f:
                content = f.read()
                zfile.writestr('df2', content)
            tmp2.close()
        memory_file.seek(0)
        return memory_file.getvalue()
