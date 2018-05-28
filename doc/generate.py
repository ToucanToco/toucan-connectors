# Script to generate a connector documentation.
import sys
import re
import os
from contextlib import suppress

import toucan_connectors


def doc_or_empty(klass):
    with suppress(AttributeError):
        return klass.__doc__.strip()


def custom_str(field):
    whitelist = ('type', 'required', 'default')
    m = {
        'type': lambda x: f'{x}',
        'required': lambda x: 'required' if x else None,
        'default': lambda x: f'default to {x}'
    }
    infos = [m[k](v) for k, v in field.info.items() if k in whitelist]
    return f'`{field.name}`: ' + ', '.join(x for x in infos if x is not None)


def snake_to_camel(name):
    name = ''.join(x.capitalize() or '_' for x in name.split('_'))
    d_replace = {
        'Mssql': 'MSSQL',
        'Sql': 'SQL',
        'sql': 'SQL'
    }
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
    doc = [f'# {klass.type} connector', doc_or_empty(klass), '## Data provider configuration']

    li = [f'* `type`: `"{klass.type}"`']
    schema_cson = {
        'type': '\'' + klass.type + '\''
    }
    for name, obj in klass.__fields__.items():
        if name == 'type':
            continue
        li.append(f'* {custom_str(obj)}')
        schema_cson[name] = '<' + name + '>'
        if obj.info['type'] == 'str':
            schema_cson[name] =  '\'' + schema_cson[name]  + '\''
    doc.append('\n'.join(li))

    li = []
    li.append('```coffee\nDATA_PROVIDERS= [')
    for key, val in schema_cson.items():
        li.append(f'  {key}:    {val}')
    li.append(',\n  ...\n]\n```')
    doc.append('\n'.join(li))

    doc.extend(['\n## Data source configuration', doc_or_empty(klass.data_source_model)])
    li = []
    schema_cson = {
        'type': '\'' + klass.type + '\''
    }
    for name, obj in klass.data_source_model.__fields__.items():
        if name == 'type':
            continue
        schema_cson[name] = '\'<' + name + '>\''
        li.append(f'* {custom_str(obj)}')
    doc.append('\n'.join(li))

    li = []
    li.append('```coffee\nDATA_SOURCES= [')
    for key, val in schema_cson.items():
        li.append(f'  {key}:    {val}')
    li.append(',\n  ...\n]\n```')
    doc.append('\n'.join(li))


    return '\n\n'.join([l for l in doc if l is not None])


def generate_summmary(connectors):
    doc = ['# Toucan Toco Connectors']
    for key, value in connectors.items():
        doc.append(f'* [{key}]({value}.md)')
    doc = '\n\n'.join([l for l in doc if l is not None])
    file_name = 'doc/connectors.md'
    with open(file_name, 'w') as file:
        file.write(doc)


def generate_all_doc():
    path = 'toucan_connectors/'
    connectors = [
        o for o in os.listdir(path)
        if os.path.isdir(os.path.join(path, o))
    ]
    connectors_ok = {}
    for connector in connectors:
        try:
            c_name = snake_to_camel(connector) + 'Connector'
            k = getattr(toucan_connectors, c_name)
        except AttributeError as e:
            print(e)
            continue
        doc = generate(k)
        file_name = os.path.join('doc/', connector + '.md')
        with open(file_name, 'w') as file:
            file.write(doc)
        connectors_ok[c_name] = connector
    generate_summmary(connectors_ok)

if __name__ == '__main__':
    if len(sys.argv)>1:
        k = getattr(toucan_connectors, sys.argv[1])
        print(generate(k))
    else:
        generate_all_doc()


