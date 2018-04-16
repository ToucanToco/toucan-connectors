# Script to generate a connector documentation.
import sys
from contextlib import suppress

import toucan_connectors


def doc_or_empty(klass):
    with suppress(AttributeError):
        return klass.__doc__.strip()


def custom_str(field):
    whitelist = ('type', 'required', 'default')
    m = {
        'type': lambda x: f'{x}',
        'required': lambda x: 'required' if x else '',
        'default': lambda x: f'default to {x}'
    }
    return f'`{field.name}`: ' + ', '.join(m[k](v) for k, v in field.info.items() if k in whitelist)


def generate(klass):
    """
    >>> from toucan_connectors import MicroStrategyConnector
    >>> print(generate(MicroStrategyConnector))
    # MicroStrategy connector

    ## Connector configuration

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
    doc = [f'# {klass.type} connector', doc_or_empty(klass), '## Connector configuration']

    li = [f'* `type`: `"{klass.type}"`']
    for name, obj in klass.__fields__.items():
        if name == 'type':
            continue
        li.append(f'* {custom_str(obj)}')
    doc.append('\n'.join(li))

    doc.extend(['\n## Data source configuration', doc_or_empty(klass.data_source_model)])
    li = []
    for name, obj in klass.data_source_model.__fields__.items():
        if name == 'type':
            continue
        li.append(f'* {custom_str(obj)}')
    doc.append('\n'.join(li))

    return '\n\n'.join([l for l in doc if l is not None])


if __name__ == '__main__':
    try:
        kname = sys.argv[1]
    except IndexError:
        raise ValueError('Please provide a connector module name')
    k = getattr(toucan_connectors, kname)
    print(generate(k))
