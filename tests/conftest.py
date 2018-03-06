import socket
import time

import pytest
from docker import APIClient


@pytest.fixture(scope='session')
def docker():
    docker = APIClient(version='auto')
    return docker


@pytest.fixture(scope='session')
def container_starter(request, docker):
    def f(image, internal_port, host_port, env=None, volume=None, command=None,
          checker_callable=None, skip_exception=None):
        docker_pull = False
        if docker_pull:
            print(f"Pulling {image} image")
            docker.pull(image)

        if volume is not None:
            host_vol, cont_vol = volume
            host_config = docker.create_host_config(
                port_bindings={internal_port: host_port},
                binds={host_vol: cont_vol})
            volumes = [cont_vol]
        else:
            host_config = docker.create_host_config(
                port_bindings={internal_port: host_port})
            volumes = None

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
            wait_for_container(checker_callable, image, skip_exception)
        return container

    return f


@pytest.fixture(scope='session')
def unused_port():
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]

    return f


def wait_for_container(checker_callable, image, skip_exception=None):
    delay = 0.01
    skip_exception = skip_exception or Exception
    for i in range(100):
        try:
            checker_callable()
            break
        except skip_exception as e:
            print(f"Waiting image to start, last exception: {e}")
            time.sleep(delay)
            delay *= 2
    else:
        pytest.fail(f"Cannot start {image} server")
