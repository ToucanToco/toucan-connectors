# This test ensures that we're on a numpy version where np.object can be used. Delete it once LTS
# support is dropped or numpy is numped.

import numpy as np
import pandas as pd


def test_numpy_version():
    df = pd.DataFrame({'a': ['coucou', 1, None]})

    assert df.dtypes['a'] == np.object
