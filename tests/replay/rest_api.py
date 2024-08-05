import requests
import re
from enum import Enum

api_host = "http://127.0.0.1:8080/api/v1/"


class JobStatus(Enum):
    RUNNING = "Running"
    STOPPED = "Stopped"
    UNKNOWN = "Unknown"


def find_keyframe(stream_id):
    result = requests.post(
        url=api_host + "keyframes/find",
        timeout=10,
        json={"source_id": stream_id, "from": None, "to": None, "limit": 1}
    )
    json_response = result.json()
    return json_response.get("keyframes")[1][0]

def find_middle_keyframe(stream_id):
    result = requests.post(
        url=api_host + "keyframes/find",
        timeout=10,
        json={"source_id": stream_id, "from": None, "to": None, "limit": 1000}
    )
    json_response = result.json()
    keyframe_index = len(json_response.get("keyframes")[1]) // 2
    return json_response.get("keyframes")[1][keyframe_index]

def find_keyframes(stream_id, fromIndex=None, toIndex=None, keyframes_count=0):
    request = {"source_id": stream_id}
    if keyframes_count > 0:
        request["limit"] = keyframes_count
    if fromIndex:
        request["from"] = fromIndex
    if toIndex:
        request["to"] = toIndex
    result = requests.post(
        url=api_host + "keyframes/find",
        timeout=10,
        json=request
    )
    json_response = result.json()
    return json_response.get("keyframes")[1]


def get_job_config(job_config, keyframe, stream_id, resulting_stream_id):
    job_config["anchor_keyframe"] = keyframe
    job_config["configuration"]["stored_stream_id"] = stream_id
    job_config["configuration"]["resulting_stream_id"] = resulting_stream_id
    return job_config


def new_job(job_config, keyframe, stream_id, resulting_stream_id):
    config = get_job_config(job_config, keyframe, stream_id, resulting_stream_id)

    result = requests.put(
        url=api_host + "job",
        json=config
    )
    json_response = result.json()
    # Todo: add check if there is a new_job parameter
    return json_response.get("new_job")


def delete_job(job_id):
    result = requests.delete(
        url=api_host + "job/{0}".format(job_id)
    )
    json_response = result.json()
    pattern = re.compile("^task [0-9]+ was cancelled$")
    return bool(pattern.match(json_response.get("error")))


def list_job(job_id):
    result = requests.get(
        url=api_host + "job/{0}".format(job_id)
    )
    json_response = result.json()
    return json_response


def list_jobs():
    result = requests.get(
        url=api_host + "job"
    )
    json_response = result.json()
    return json_response


def list_stopped_jobs():
    result = requests.get(
        url=api_host + "job/stopped"
    )
    json_response = result.json()
    return json_response


def get_job_status(job_id):
    running_job = list_job(job_id).get("jobs")
    running_job_id = (running_job[0][0] if len(running_job) > 0 else None)
    if running_job_id == job_id:
        return JobStatus.RUNNING

    stopped_jobs = list_stopped_jobs().get("stopped_jobs")
    for stopped_job in stopped_jobs:
        if stopped_job[0] == job_id:
            return JobStatus.STOPPED

    return JobStatus.UNKNOWN
