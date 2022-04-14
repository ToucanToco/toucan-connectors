# Script to generate a connector documentation.
import collections
import os
import sys
from contextlib import suppress
from importlib import import_module


def doc_or_empty(klass):
    with suppress(AttributeError):
        return klass.__doc__.strip()


def clean_type_str(field_str):
    Constr_values = {
        'ConstrainedStrValue': 'str (not empty)',
        'ConstrainedIntValue': 'int (not empty)',
    }
    field_str = f'{field_str}'.replace("<class '", "").replace("'>", "").replace("<enum '", "")
    field_str = field_str.split('.')
    if field_str[-1] in Constr_values.keys():
        return Constr_values[field_str[-1]]
    if len(field_str) > 1:
        return field_str[-1]
    else:
        return field_str[0]


def custom_str(field):
    # whitelist = ('type_', 'required', 'default')
    m = {
        'type_': lambda x: clean_type_str(x) if x else None,
        'required': lambda x: 'required' if x else None,
        'default': lambda x: f'default to {x}' if x is not None else x,
    }
    infos = []
    infos.append(m['type_'](field.type_))
    infos.append(m['required'](field.required))
    infos.append(m['default'](field.default))
    return f'`{field.name}`: ' + ', '.join(x for x in infos if x is not None)


def snake_to_camel(name):
    name = ''.join(x.capitalize() or '_' for x in name.split('_'))
    d_replace = {'Mssql': 'MSSQL', 'Sql': 'SQL', 'sql': 'SQL'}
    for key, val in d_replace.items():
        name = name.replace(key, val)
    return name


def generate(klass):
    """
    >>> from toucan_connectors import MicroStrategyConnector
    >>> print(generate(MicroStrategyConnector))
    # MicroStrategy connector

    ## Data provider configuration

    * `type`: "MicroStrategy"
    * `name`: str, required
    * `base_url`: str, required
    * `username`: str, required
    * `password`: str, required
    * `project_id`: str, required


    ## Data source configuration

    * `domain`: str, required
    * `name`: str, required
    * `id`: str, required
    * `dataset`: Dataset, required
    """
    klassname = klass.__name__.replace('Connector', '')
    # Retrieving class name from __name__ as type is no longer in the right format
    doc = [f'# {klassname} connector', doc_or_empty(klass), '## Data provider configuration']

    li = [f'* `type`: `"{klassname}"`']
    schema_cson = {'type': f"'{klassname}'"}
    for name, obj in klass.__fields__.items():
        if name == 'label' or name == 'retry_policy':
            continue
        li.append(f'* {custom_str(obj)}')
        schema_cson[name] = f"'<{name}>'"
    doc.append('\n'.join(li))

    li = []
    li.append('```coffee\nDATA_PROVIDERS: [')
    for key, val in schema_cson.items():
        li.append(f'  {key}:    {val}')
    li.append(',\n  ...\n]\n```')
    doc.append('\n'.join(li))

    doc.extend(['\n## Data source configuration', doc_or_empty(klass.data_source_model)])
    li = []
    schema_cson = {}
    for name, obj in klass.data_source_model.__fields__.items():
        if name in ['type', 'load', 'live_data', 'validation', 'parameters']:
            continue
        schema_cson[name] = f"'<{name}>'"
        li.append(f'* {custom_str(obj)}')
    doc.append('\n'.join(li))

    li = []
    li.append('```coffee\nDATA_SOURCES: [')
    for key, val in schema_cson.items():
        if name == 'parameters':
            continue
        li.append(f'  {key}:    {val}')
    li.append(',\n  ...\n]\n```')
    doc.append('\n'.join(li))

    return '\n\n'.join([line for line in doc if line is not None])


def get_connectors():
    path = 'toucan_connectors/'
    connectors = [
        o for o in os.listdir(path) if os.path.isdir(os.path.join(path, o)) & (o != '__pycache__')
    ]
    connectors_ok = {}
    for connector in connectors:
        try:
            c_name = f'{snake_to_camel(connector)}Connector'
            mod = import_module(f'{path[:-1]}.{connector}.{connector}_connector', path[:-1])
            getattr(mod, c_name)
            connectors_ok[c_name] = connector
            try:
                c_name = dir(f'{path[:-1]}.{connector}.{connector}_connector')
            except Exception:
                continue
        except AttributeError:
            continue
        except ModuleNotFoundError:
            continue

    return connectors_ok


def generate_summmary(connectors):
    doc = ['# Toucan Connectors']
    connectors = collections.OrderedDict(sorted(connectors.items()))
    for key, value in connectors.items():
        doc.append(f'* [{key}](connectors/{value}.md)')
    doc = '\n\n'.join([line for line in doc if line is not None])
    file_name = 'doc/connectors.md'
    with open(file_name, 'w') as file:
        file.write(doc)


def generate_all_doc(connectors):
    for key, value in connectors.items():
        mod = import_module(f'toucan_connectors.{value}.{value}_connector', 'toucan_connectors')
        k = getattr(mod, key)
        doc = generate(k)
        file_name = os.path.join('doc/connectors/', f'{value}.md')
        with open(file_name, 'w') as file:
            file.write(doc)


if __name__ == '__main__':
    connectors = get_connectors()
    if len(sys.argv) > 1:
        con = sys.argv[1]
        mod = import_module(f'toucan_connectors.{con}.{con}_connector', 'toucan_connectors')
        k = getattr(mod, f'{snake_to_camel(sys.argv[1])}Connector')
        print(generate(k))
    else:
        generate_all_doc(connectors)
    generate_summmary(connectors)
