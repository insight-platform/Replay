import pytest
import docker
import os
import time
import json
from savant.client import SinkBuilder, JaegerLogProvider

import replay.rest_api as rest_api

replay_image = "ghcr.io/insight-platform/replay-x86:v0.7.1"
savant_adapters_gstreamer_image = "ghcr.io/insight-platform/savant-adapters-gstreamer:latest"


@pytest.fixture(scope='session', autouse=True)
def project_root(pytestconfig):
    """Return path to the project root"""
    yield str(pytestconfig.rootpath)


@pytest.fixture(scope='session', autouse=True)
def docker_client():
    """Initialize docker client, update docker images and get project root"""

    docker_client = docker.from_env()
    docker_client.images.pull(replay_image)
    docker_client.images.pull(savant_adapters_gstreamer_image)

    yield docker_client


@pytest.fixture(scope='session', autouse=True)
def replay_setup(docker_client, project_root):
    """Start Replay at the beginning of a session and stop it at the end"""

    # Replay setup: run docker container
    replay = docker_client.containers.run(
        replay_image,
        name="replay",
        detach=True,
        remove=True,
        network="host",
        volumes=[
            "{0}/replay/config.json:/opt/etc/config.json".format(project_root),
            "{0}/replay/data:/opt/rocksdb".format(project_root)
        ],
        environment=[
            "RUST_LOG=info"
        ]
    )

    yield

    # Replay teardown: stop docker container
    replay.stop()


@pytest.fixture(scope='function', autouse=True)
def stream_id(request):
    """Calculate stream_id based on test name"""
    node_path = request.node.nodeid.split('::')
    yield "{0}_{1}_video".format(node_path[1], node_path[2])


@pytest.fixture(scope='function', autouse=True)
def video_loop_setup(docker_client, project_root, stream_id):
    """Start Video Loop before each test and stop it after the test"""

    # Video Loop setup: run docker container
    container_name = "video_loop_{0}".format(time.monotonic_ns())
    video_loop = docker_client.containers.run(
        savant_adapters_gstreamer_image,
        name=container_name,
        detach=True,
        remove=True,
        network="host",
        volumes=[
            "{0}/video_loop/source_downloads:/tmp/video-loop-source-downloads".format(project_root),
            "{0}/test_data/test_video.avi:/tmp/test_data/test_video.avi".format(project_root)
        ],
        entrypoint="/opt/savant/adapters/gst/sources/video_loop.sh",
        environment=[
            "LOCATION=/tmp/test_data/test_video.avi",
            "DOWNLOAD_PATH=/tmp/video-loop-source-downloads",
            "ZMQ_ENDPOINT=dealer+connect:tcp://127.0.0.1:5555",
            "SOURCE_ID={0}".format(stream_id),
            "SYNC_OUTPUT=True"
        ]
    )
    # todo: perhaps to check containers status instead of simple sleep
    time.sleep(3)

    yield

    # Docker teardown: stopping the containers
    video_loop.stop()


@pytest.fixture(scope='function')
def replay_job(job_config, stream_id):
    """Start a new job before and stop it after the test.
    The 1st keyframe is used."""

    # Test Setup: start a new job
    # > Find keyframe
    keyframe = rest_api.find_keyframe(stream_id)
    # > Make up resulting stream_id
    resulting_stream_id = "{0}_result".format(stream_id)
    # > Start Replay job
    job_id = rest_api.new_job(job_config, keyframe, stream_id, resulting_stream_id)

    # Run a test
    yield job_id

    # Test teardown: delete the job
    rest_api.delete_job(job_id)


@pytest.fixture(scope='function')
def replay_job_middle(job_config, stream_id):
    """Start a new job before and stop it after the test
    Keyframe from the middle is used."""

    # Test Setup: start a new job
    # > Find keyframe
    keyframe = rest_api.find_middle_keyframe(stream_id)
    # > Make up resulting stream_id
    resulting_stream_id = "{0}_result".format(stream_id)
    # > Start Replay job
    job_id = rest_api.new_job(job_config, keyframe, stream_id, resulting_stream_id)

    # Run a test
    yield job_id


    # Test teardown: delete the job
    rest_api.delete_job(job_id)


@pytest.fixture(scope='function')
def job_config(project_root, request):
    """Find replay job config based on test name"""

    default_path = os.path.join(project_root, 'test_data/job_config/default.json')
    node_path = request.node.nodeid.split('::')
    config_path = os.path.join(project_root, 'test_data', 'job_config', node_path[1], node_path[2] + '.json')
    if not os.path.isfile(config_path):
        config_path = default_path
    if not os.path.isfile(config_path):
        pytest.skip("Config file {0} does not exist".format(config_path))
    with open(config_path, "r") as f:
        config = json.load(f)
    yield config


@pytest.fixture(scope='function')
def sink_runner(replay_setup, video_loop_setup):
    time.sleep(60)
    sink = (SinkBuilder()
            .with_socket('router+bind:tcp://0.0.0.0:6666')
            .with_idle_timeout(20)
            .build()
            )

    yield sink

    del sink

#def pytest_addoption(parser):
#    parser.addoption("--network", action="store", default="night-stand", help="Which stand use")
#    parser.addoption("--envs", action="store", default="envs.json", help="Filename with environments")


#def pytest_configure(config: Config):
#    network_name = config.getoption("--network")
#    envs_file = config.getoption("--envs")
#    with open(pathlib.Path().parent.parent / envs_file, "r+") as f:
#        environments = json.load(f)
#    assert network_name in environments, f"Environment {network_name} doesn't exist in envs.json"
#    config.environment = EnvironmentConfig(**environments[network_name])
