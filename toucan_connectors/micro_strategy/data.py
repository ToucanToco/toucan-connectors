from copy import deepcopy
from functools import singledispatch


def get_definition(results):
    dfn = deepcopy(results['result']['definition'])
    attrs = dfn['attributes']
    for attr in dfn['attributes']:
        if 'forms' in attr:
            attr['forms'] = {f['name']: f for f in attr['forms']}
    dfn['attributes_id'] = {attr['id']: attr for attr in attrs}  # attr ID as key
    dfn['attributes'] = {attr['name']: attr for attr in attrs}  # attr name as key
    dfn['metrics_id'] = {m['id']: m for m in dfn['metrics']}
    dfn['metrics'] = {m['name']: m for m in dfn['metrics']}
    return dfn


def fill_viewfilter_with_ids(vf, dfn):
    def fill_attribute(attr_name):
        if '@' in attr_name:
            attr_name, form_name = attr_name.split('@')
            try:
                dfn_attr = dfn['attributes'][attr_name]
            except KeyError:
                # No attribute has this name, so it must be a raw ID:
                dfn_attr = dfn['attributes_id'][attr_name]
            return {
                'type': 'form',
                'attribute': {'id': dfn_attr['id']},
                'form': {'id': dfn_attr['forms'][form_name]['id']},
            }
        else:
            try:
                dfn_attr = dfn['attributes'][attr_name]
            except KeyError:
                # No attribute has this name, so it must be a raw ID:
                dfn_attr = dfn['attributes_id'][attr_name]
            return {'type': 'attribute', 'id': dfn_attr['id']}

    def fill_metric(metric_name):
        try:
            dfn_metric = dfn['metrics'][metric_name]
        except KeyError:
            dfn_metric = dfn['metrics_id'][metric_name]
        return {'type': 'metric', 'id': dfn_metric['id']}

    def fill_constant(constant, data_type):
        data_type = data_type or ('Char' if isinstance(constant, str) else 'Real')
        return {'type': 'constant', 'dataType': data_type, 'value': str(constant)}

    @singledispatch
    def visit(_):
        pass

    @visit.register(dict)
    def visit_dict(d: dict):
        for v in d.values():
            visit(v)
        if 'attribute' in d:
            d.update(**fill_attribute(d.pop('attribute')))
        elif 'metric' in d:
            d.update(**fill_metric(d.pop('metric')))
        elif 'constant' in d:
            d.update(**fill_constant(d.pop('constant'), d.get('dataType')))

    @visit.register(list)
    def visit_list(node: list):
        for e in node:
            visit(e)

    vf = deepcopy(vf)
    visit(vf)
    return vf


def get_attr_names(data: dict) -> dict:
    """Retrieves attribute names from returned data."""
    row = {}
    for index, col in enumerate(data['result']['definition']['attributes']):
        row[index] = col['name']
    return row


def get_metric_names(data: dict) -> dict:
    """Retrieves metric names from returned data."""
    row = {}
    for index, col in enumerate(data['result']['definition']['metrics']):
        row[index] = col['name']
    return row


def flatten_json(json_root: dict, attributes: dict, metrics: dict) -> list:
    """Entry into recursive function to pull data from JSON based on attributes & metrics."""
    row = {}
    table = []

    def flatten(nodes: dict, attributes: dict, metrics: dict, row: dict, table: list):
        """Recursive function that will traverse JSON to flatten data."""
        if isinstance(nodes, dict):
            for node in nodes:
                # it appears 'depth' is an indicator when data elements are coming
                if node == 'depth':
                    row[attributes[nodes['depth']]] = nodes['element']['name']
                    # also, the 'depth' value appears to be associated to the number of attributes,
                    # so we use this to determine when to get the metric data and write the row to
                    # the table
                    if nodes['depth'] == (len(attributes) - 1):
                        # iterate through metric names to get the values in json
                        for value in metrics.values():
                            row[value] = nodes['metrics'][value]['rv']
                        table.append(row.copy())
                flatten(nodes[node], attributes, metrics, row, table)
        elif isinstance(nodes, list):
            for node in nodes:
                flatten(node, attributes, metrics, row, table)

    flatten(json_root, attributes, metrics, row, table)
    return table
