from pathlib import Path


def get_install_script_path(connector_name):
    return Path(__file__).parent.resolve() / f"{connector_name}.sh"
