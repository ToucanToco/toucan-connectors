def get_attr_names(data: dict) -> dict:
    """Retrieves attribute names from returned data."""
    row = {}
    for index, col in enumerate(data['result']['definition']['attributes']):
        row[index] = col["name"]
    return row


def get_metric_names(data: dict) -> dict:
    """Retrieves metric names from returned data."""
    row = {}
    for index, col in enumerate(data['result']['definition']['metrics']):
        row[index] = col["name"]
    return row


def flatten_json(json_root: dict, attributes: dict, metrics: dict) -> list:
    """ Entry into recursive function to pull data from JSON based on attributes & metrics."""
    row = {}
    table = []

    def flatten(nodes: dict, attributes: dict, metrics: dict, row: dict, table: list):
        """ Recursive function that will traverse JSON to flatten data."""
        if isinstance(nodes, dict):
            for node in nodes:
                # it appears "depth" is an indicator when data elements are coming
                if node == "depth":
                    row[attributes[nodes["depth"]]] = nodes["element"]["name"]
                    # also, the "depth" value appears to be associated to the number of attributes,
                    # so we use this to determine when to get the metric data and write the row to
                    # the table
                    if nodes["depth"] == (len(attributes) - 1):
                        # iterate through metric names to get the values in json
                        for value in metrics.values():
                            row[value] = nodes["metrics"][value]["rv"]
                        table.append(row.copy())
                flatten(nodes[node], attributes, metrics, row, table)
        elif isinstance(nodes, list):
            for node in nodes:
                flatten(node, attributes, metrics, row, table)

    flatten(json_root, attributes, metrics, row, table)
    return table
