define(`upcase', `translit(`$*', `a-z', `A-Z')')dnl
define(`downcase', `translit(`$*', `A-Z', `a-z')')dnl
define(`cap', `regexp(`$1', `^\(\w\)\(\w*\)', `upcase(`\1')`'downcase(`\2')')')dnl
import pytest

from toucan_connectors.name.`'name`'_connector import cap(name)Connector, cap(name)DataSource


def test_get_df():
    pass

