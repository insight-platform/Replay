import pytest

import time
import numpy as np

import replay.rest_api as rest_api


class TestJob:
    n = 20
    margin_nano_min = 500000
    margin_nano_max = 1000000
    margin_nano_stop = 6000000000

    def test_general_check(self, replay_job, sink_runner):
        test_result = False
        for result in sink_runner:
            test_result = True
            break
        assert test_result

    @pytest.mark.skip(reason="not implemented")
    @pytest.mark.configuration
    def test_configuration_ts_sync(self):
        pass

    @pytest.mark.skip(reason="not implemented")
    @pytest.mark.configuration
    def test_configuration_skip_intermediary_eos(self):
        pass

    @pytest.mark.configuration
    def test_configuration_send_eos_true(self, job_config, replay_job, sink_runner):
        # read configuration > stop condition > frame count from job config
        max_frames_count = job_config.get("stop_condition", {}).get("frame_count")
        if not max_frames_count:
            pytest.skip("Job stop condition is expected to be by frames count")
        eos = None
        i = 0
        for result in sink_runner:
            i += 1
            if result.eos:
                eos = True
                break
        assert i == max_frames_count + 1 and eos

    @pytest.mark.configuration
    def test_configuration_send_eos_false(self, job_config, replay_job, sink_runner):
        # read configuration > stop condition > frame count from job config
        max_frames_count = job_config.get("stop_condition", {}).get("frame_count")
        if not max_frames_count:
            pytest.skip("Job stop condition is expected to be by frames count")
        eos = None
        i = 0
        for result in sink_runner:
            i += 1
            eos = (result.eos is not None)
            job_status = rest_api.get_job_status(replay_job)
            # Most certainly job status will still be Running after all frames are delivered
            # because it takes time to stop it, and the loop will continue to the next iteration.
            # In this case the loop will be finished by sink_runner idle timeout
            if job_status != rest_api.JobStatus.RUNNING:
                break
        assert i == max_frames_count and not eos

    @pytest.mark.skip(reason="not implemented")
    @pytest.mark.configuration
    def test_configuration_stop_on_incorrect_ts(self):
        pass

    @pytest.mark.skip(reason="not implemented")
    @pytest.mark.configuration
    def test_configuration_ts_discrepancy_fix_duration(self):
        pass

    @pytest.mark.configuration
    def test_configuration_min_duration(self, job_config, replay_job, sink_runner):
        # calculate min_duration_nano from job_config
        min_duration = job_config.get("configuration").get("min_duration")
        min_duration_nano = int(min_duration.get("secs") or 0) * 1000000000 + int(min_duration.get("nanos") or 0)

        test_result = True
        i = 0
        frame_timestamp = []
        for result in sink_runner:
            if result.eos:
                print('EOS')
                break
            frame_timestamp.append(time.monotonic_ns())
            i += 1
            if i >= self.n:
                break
        if i == 0:
            pytest.fail("No frames for testing")

        timestamp_min_diff = np.diff(frame_timestamp)
        for diff in timestamp_min_diff:
            if abs(diff - min_duration_nano) > self.margin_nano_min:
                test_result = False

        assert test_result

    def test_configuration_max_duration(self, job_config, replay_job, sink_runner):
        # calculate max_duration_nano from job_config
        max_duration = job_config.get("configuration").get("max_duration")
        max_duration_nano = int(max_duration.get("secs") or 0) * 1000000000 + int(max_duration.get("nanos") or 0)

        test_result = True
        i = 0
        frame_timestamp = []
        for result in sink_runner:
            if result.eos:
                break
            frame_timestamp.append(time.monotonic_ns())
            i += 1
            if i >= self.n:
                break
        if (i == 0):
            pytest.fail("No frames for testing")
        timestamp_max_diff = np.diff(frame_timestamp)
        for diff in timestamp_max_diff:
            if abs(diff - max_duration_nano) > self.margin_nano_max:
                test_result = False
        assert test_result

    @pytest.mark.skip(reason="not implemented")
    @pytest.mark.configuration
    def test_configuration_routing_labels_bypass(self):
        pass

    @pytest.mark.skip(reason="not implemented")
    @pytest.mark.configuration
    def test_configuration_routing_labels_replace(self):
        pass

    @pytest.mark.skip(reason="not implemented")
    @pytest.mark.configuration
    def test_configuration_routing_labels_append(self):
        pass

    @pytest.mark.skip(reason="not implemented")
    @pytest.mark.configuration
    def test_configuration_max_idle_duration(self):
        pass

    @pytest.mark.skip(reason="not implemented")
    @pytest.mark.configuration
    def test_configuration_max_delivery_duration(self):
        pass

    @pytest.mark.configuration
    def test_configuration_send_metadata_only_true(self, replay_job, sink_runner):
        test_result = True
        i = 0
        for result in sink_runner:
            if result.eos:
                break
            test_result = (result.frame_content is None)
            i += 1
            if i >= self.n or not test_result:
                break
        if (i == 0):
            pytest.fail("No frames for testing")

        assert test_result

    @pytest.mark.configuration
    def test_configuration_send_metadata_only_false(self, replay_job, sink_runner):
        test_result = True
        i = 0
        for result in sink_runner:
            if result.eos:
                break
            test_result = (result.frame_content is not None)
            i += 1
            if i >= self.n or not test_result:
                break
        if i == 0:
            pytest.fail("No frames for testing")
        assert test_result

    @pytest.mark.configuration
    def test_configuration_labels(self, job_config, replay_job):
        expected_labels = job_config.get("configuration").get("labels")
        labels = rest_api.list_job(replay_job).get("jobs")[0][1].get("labels")
        assert labels == expected_labels

    def test_anchor_keyframe(self, job_config, replay_job_middle, sink_runner):
        expected_anchor_keyframe = job_config.get("anchor_keyframe")
        frame_uuid = ""
        for result in sink_runner:
            frame_uuid = result.frame_meta.uuid
            break
        assert frame_uuid == expected_anchor_keyframe

    # Offset

    @pytest.mark.skip(reason="constantly failing")
    @pytest.mark.offset
    def test_offset_blocks(self, job_config, replay_job_middle, sink_runner, stream_id):
        blocks_offset = job_config.get("offset", {}).get("blocks")
        if not blocks_offset:
            pytest.skip("Job blocks offset is expected to be specified")
        anchor_keyframe = job_config.get("anchor_keyframe")
        print ("anchor keyframe: ", anchor_keyframe)

        keyframes = rest_api.find_keyframes(stream_id, keyframes_count=500)
        print ("keyframes: ", keyframes)

        i = 0
        j = 0
        for result in sink_runner:
            print (result.frame_meta.uuid, "(", result.frame_meta.keyframe, ")")
            if result.frame_meta:
                j += 1
            if result.frame_meta and result.frame_meta.keyframe:
                i += 1
            if result.frame_meta.uuid == anchor_keyframe:
                break
        print ("keyframes: ", i, ", frames: ", j)
        assert i == blocks_offset + 1

    # precondition: `"offset": { "seconds": 5},`
    @pytest.mark.skip(reason="not implemented")
    @pytest.mark.offset
    def test_offset_seconds(self, job_config, replay_job_middle, sink_runner):
        pass

    # Attributes

    @pytest.mark.attributes
    def test_attributes_hint(self, job_config, replay_job, sink_runner):
        expected_hint = "Test hint"
        hint = None
        for result in sink_runner:
            if result.eos:
                print('EOS')
                break
            hint = result.frame_meta.get_attribute('key', 'value').hint
            break
        assert hint == expected_hint

    # Configuration > Stop condition

    @pytest.mark.stop_condition
    def test_stop_condition_last_frame(self, job_config, replay_job, sink_runner):
        # read configuration > stop condition > frame count from job config
        expected_last_frame = job_config.get("stop_condition", {}).get("last_frame", {}).get("uuid")
        if not expected_last_frame:
            pytest.skip("Job stop condition is expected to be by last frame")

        pre_last_frame = ""
        last_frame = ""
        for result in sink_runner:
            if result.eos:
                break
            pre_last_frame = last_frame
            last_frame = result.frame_meta.uuid

        job_status = rest_api.get_job_status(replay_job)
        assert (
                job_status == rest_api.JobStatus.STOPPED
                and pre_last_frame < expected_last_frame <= last_frame
        )

    @pytest.mark.stop_condition
    def test_stop_condition_key_frame_count(self, stream_id, job_config, replay_job, sink_runner):
        # read configuration > stop condition > frame count from job config
        max_keyframes_count = job_config.get("stop_condition", {}).get("key_frame_count")
        if not max_keyframes_count:
            pytest.skip("Job stop condition is expected to be by keyframes count")

        #pytest.fail("force failure")

        keyframes = rest_api.find_keyframes(stream_id, keyframes_count=max_keyframes_count)
        max_keyframes_count = min(max_keyframes_count, len(keyframes))
        i = 0
        for result in sink_runner:
            if result.frame_meta and result.frame_meta.keyframe:
                i += 1
        job_status = rest_api.get_job_status(replay_job)
        assert (
                job_status == rest_api.JobStatus.STOPPED
                and i == max_keyframes_count
        )

    @pytest.mark.skip("Wrong implementation")
    @pytest.mark.stop_condition
    def test_stop_condition_timestamp_delta(self, job_config, replay_job, sink_runner):
        # read configuration > stop condition > timestamp delta from job config
        timestamp_delta = job_config.get("stop_condition", {}).get("ts_delta_sec", {}).get("max_delta_sec") * 1000000000
        if not timestamp_delta:
            pytest.skip("Job stop condition is expected to be by timestamp delta")

        frame_timestamp = []
        for result in sink_runner:
            if result.eos:
                break
            frame_timestamp.append(time.monotonic_ns())
        timestamp_diff = np.diff(frame_timestamp)

        test_result = True
        for diff in timestamp_diff:
            if diff > timestamp_delta:
                test_result = False
        job_status = rest_api.get_job_status(replay_job)
        assert (
                job_status == rest_api.JobStatus.STOPPED
                and test_result
        )

    @pytest.mark.stop_condition
    def test_stop_condition_realtime_delta(self, job_config, replay_job, sink_runner):
        start_timestamp = time.monotonic_ns()
        # read configuration > stop condition > timestamp delta from job config
        realtime_delta = job_config.get("stop_condition", {}).get("real_time_delta_ms", {}).get("configured_delta_ms") * 1000000
        if not realtime_delta:
            pytest.skip("Job stop condition is expected to be by realtime delta")

        end_timestamp = time.monotonic_ns()
        for result in sink_runner:
            end_timestamp = time.monotonic_ns()
        job_lifetime = end_timestamp - start_timestamp
        time.sleep(5)
        job_status = rest_api.get_job_status(replay_job)
        assert (
                job_status == rest_api.JobStatus.STOPPED
                and realtime_delta < job_lifetime < realtime_delta + self.margin_nano_stop
        )
