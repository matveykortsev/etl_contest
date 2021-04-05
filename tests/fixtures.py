import time
import pytest   # noqa
import docker   # noqa
from .helpers import (get_session_id,
                      get_unused_port,
                      ping_container,
                      load_assets_to_source_db,
                      load_struct_to_destination_db)


BASE_DOCKER_IMAGE = 'percona/percona-server:5.7.32'


@pytest.fixture
def base_mysql_image() -> dict:

    session_id = get_session_id()
    docker_client = docker.client.from_env()
    unused_port = get_unused_port()

    credentials = {'host': 'localhost',
                   'port': unused_port,
                   'database': 'sandbox',
                   'user': 'etl',
                   'password': 'etl_contest',
                   'autocommit': True}

    env = {'MYSQL_DATABASE': 'sandbox',
           'MYSQL_USER': 'etl',
           'MYSQL_PASSWORD': 'etl_contest',
           'MYSQL_ROOT_PASSWORD': 'root_etl_contest'}

    ports = {3306: unused_port}

    docker_client.images.pull(BASE_DOCKER_IMAGE)

    container = docker_client.containers.run(
        image=BASE_DOCKER_IMAGE,
        ports=ports,
        environment=env,
        name=session_id,
        detach=True)

    time.sleep(2)
    ping_container(credentials, session_id)

    #   yielding credentials
    yield credentials

    #   teardown part
    container.stop()
    time.sleep(2)
    docker_client.close()


@pytest.fixture
def mysql_source_image(base_mysql_image):
    load_assets_to_source_db(base_mysql_image)
    return base_mysql_image


@pytest.fixture
def mysql_destination_image(base_mysql_image):
    load_struct_to_destination_db(base_mysql_image)
    return base_mysql_image
