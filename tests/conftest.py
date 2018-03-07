import socket
import time
from os import path

import pytest
import yaml
from docker import APIClient


def pytest_addoption(parser):
    parser.addoption('--no-pull', action='store_true', default=False,
                     help='Do not pull docker images')


@pytest.fixture(scope='session')
def docker_pull(request):
    return not request.config.getoption('--no-pull')


@pytest.fixture(scope='session')
def docker():
    docker = APIClient(version='auto')
    return docker


@pytest.fixture(scope='session')
def unused_port():
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]

    return f


def wait_for_container(checker_callable, host_port, image, skip_exception=None):
    delay = 0.01
    skip_exception = skip_exception or Exception
    for i in range(100):
        try:
            checker_callable(host_port)
            break
        except skip_exception as e:
            print(f'Waiting image to start, last exception: {e}')
            time.sleep(delay)
            delay *= 2
    else:
        pytest.fail(f'Cannot start {image} server')


@pytest.fixture(scope='session')
def container_starter(request, docker, docker_pull):
    def f(image, internal_port, host_port, env=None, volumes=None, command=None,
          checker_callable=None, skip_exception=None):
        if docker_pull:
            print(f'Pulling {image} image')
            docker.pull(image)

        host_config = docker.create_host_config(
            port_bindings={internal_port: host_port},
            binds=volumes
        )
        volumes = [vol.split(':')[1] for vol in volumes]

        container_name = f'toucan-connectors-{image.replace(":", "-")}-server'
        print(f'Creating {container_name}')
        container = docker.create_container(
            image=image,
            name=container_name,
            ports=[internal_port],
            detach=True,
            environment=env,
            volumes=volumes,
            command=command,
            host_config=host_config)

        print(f'Starting {container_name}')
        docker.start(container=container['Id'])

        def fin():
            print(f'Stopping {container_name}')
            docker.kill(container=container['Id'])
            print(f'Killing {container_name}')
            docker.remove_container(container['Id'], v=True)

        request.addfinalizer(fin)
        container['port'] = host_port

        if checker_callable is not None:
            wait_for_container(checker_callable, host_port, image, skip_exception)
        return container

    return f


@pytest.fixture(scope='session')
def service_container(unused_port, container_starter):
    def f(service_name, checker_callable=None, skip_exception=None):
        with open(f'{path.dirname(__file__)}/docker-compose.yml') as docker_comppse_yml:
            docker_conf = yaml.load(docker_comppse_yml)
        service_conf = docker_conf[service_name]
        volumes = service_conf.get('volumes')
        if volumes is not None:
            volumes = [path.join(path.dirname(__file__), vol) for vol in volumes]
        params = {
            'image': service_conf['image'],
            'internal_port': service_conf['ports'][0].split(':')[0],
            'host_port': unused_port(),
            'env': service_conf.get('environment'),
            'volumes': volumes,
            'command': service_conf.get('command'),
            'checker_callable': checker_callable,
            'skip_exception': skip_exception
        }

        return container_starter(**params)

    return f
