REST API
============

Frame UUID
----------

In Savant, every frame has strictly increasing UUID, constructed as UUIDv7. The UUID is created when the frame is created and most processing nodes just reuse it without alteration. It allows us to know precisely the time of the frame creation and the order of the frames.

Replay uses frame UUIDs to navigate the video stream. The UUIDs are used to find keyframes, create jobs and update job stop conditions.

Status
------

Returns the status of the server.

.. code-block:: bash

    curl -X GET http://127.0.0.1:8080/api/v1/status

.. list-table::
    :header-rows: 1

    * - Parameter
      - Description
      - Extra
      - Description
    * - Method
      - GET
      - /api/v1/status
      -
    * - Response OK (Running)
      - 200 OK
      - ``"running"``
      - service is functional and can be used
    * - Response OK (Finished)
      - 200 OK
      - ``"finished"``
      - service is functional but cannot be used anymore
    * - Response Error
      - 500 Internal Server Error
      - ``{"error" => "Reason"}``
      - service is not functional

Endpoint: GET /api/v1/status

Shutdown
--------

Stops the server.

.. code-block:: bash

    curl -X POST http://127.0.0.1:8080/api/v1/shutdown

.. list-table::
    :header-rows: 1

    * - Parameter
      - Description
      - Extra
      - Description
    * - Method
      - POST
      - /api/v1/shutdown
      -
    * - Response OK
      - 200 OK
      - ``"ok"``
      - service is shutting down

Find Keyframes
--------------

Finds keyframes in a video stream. Returns found keyframes from oldest to newest.

.. code-block:: bash

    curl --header "Content-Type: application/json" -X POST \
         --data '{"source_id": "in-video", "from": null, "to": null, "limit": 1}' \
         http://127.0.0.1:8080/api/v1/keyframes/find | json_pp

.. list-table::
    :header-rows: 1

    * - Parameter
      - Description
      - Extra
      - Description
    * - Method
      - POST
      - /api/v1/keyframes/find
      -
    * - Request
      - JSON
      - ``{...}``
      - see below
    * - ``source_id``
      - string
      - ``"in-video"``
      - source identifier
    * - ``from``
      - int
      - ``null``
      - start time in seconds (Unix time), optional
    * - ``to``
      - int
      - ``null``
      - end time in seconds (Unix time), optional
    * - ``limit``
      - int
      - ``1``
      - maximum number of keyframes UUIDs to return. Must be a positive integer.
    * - Response OK
      - 200 OK
      - ``{"keyframes" : ["in-video", ["018f76e3-a0b9-7f67-8f76-ab0402fda78e", ...]]}``
      - list of keyframes UUIDs
    * - Response Error
      - 500 Internal Server Error
      - ``{"error" => "Reason"}``
      - problem description

Create New Job
--------------

Creates a new job. Returns the job UUID.

.. code-block:: bash

    #!/bin/bash

    query() {

    ANCHOR_KEYFRAME=$1

    cat <<EOF
    {
      "sink": {
        "url": "pub+connect:tcp://127.0.0.1:6666",
        "send_timeout": {
          "secs": 1,
          "nanos": 0
        },
        "send_retries": 3,
        "receive_timeout": {
          "secs": 1,
          "nanos": 0
        },
        "receive_retries": 3,
        "send_hwm": 1000,
        "receive_hwm": 1000,
        "inflight_ops": 100
      },
      "configuration": {
        "ts_sync": true,
        "skip_intermediary_eos": false,
        "send_eos": true,
        "stop_on_incorrect_ts": false,
        "ts_discrepancy_fix_duration": {
          "secs": 0,
          "nanos": 33333333
        },
        "min_duration": {
          "secs": 0,
          "nanos": 10000000
        },
        "max_duration": {
          "secs": 0,
          "nanos": 103333333
        },
        "stored_stream_id": "in-video",
        "resulting_stream_id": "vod-video-1",
        "routing_labels": "bypass",
        "max_idle_duration": {
          "secs": 10,
          "nanos": 0
        },
        "max_delivery_duration": {
          "secs": 10,
          "nanos": 0
        },
        "send_metadata_only": false,
        "labels": {
            "namespace": "key"
        }
      },
      "stop_condition": {
        "frame_count": 10000
      },
      "anchor_keyframe": "$ANCHOR_KEYFRAME",
      "offset": {
        "blocks": 5
      },
      "attributes": [
        {
          "namespace": "key",
          "name": "value",
          "values": [
            {
              "confidence": 0.5,
              "value": {
                "Integer": 1
              }
            },
            {
              "confidence": null,
              "value": {
                "FloatVector": [
                  1.0,
                  2.0,
                  3.0
                ]
              }
            }
          ],
          "hint": null,
          "is_persistent": true,
          "is_hidden": false
        }
      ]
    }
    EOF

    }

    Q=$(query $1)
    curl -X PUT -H "Content-Type: application/json" -d "$Q" http://127.0.0.1:8080/api/v1/job | json_pp

Augmenting Attributes
^^^^^^^^^^^^^^^^^^^^^

Attributes are defined in JSON format matching savant-rs `Attribute` struct. For details, please take a look at the `Attribute` struct in the `savant-rs <https://insight-platform.github.io/savant-rs/modules/savant_rs/primitives.html#savant_rs.primitives.Attribute>`_ documentation and the relevant `sample <https://github.com/insight-platform/savant-rs/blob/main/python/primitives/attribute.py>`_.

Attributes are passed to the job automatically ingested in every frame metadata to give the stream receiver extra knowledge about the job. For example, you can pass the track ID for the object you want to handle additionally.

Job Labels
^^^^^^^^^^

These labels are used for the user need. When you have a lot of concurrent jobs you may want to associate some metadata with them.

.. code-block:: javascript

    "labels": {
      "key": "value"
    }

When you request the information about the running or stopped jobs, you can effectively distinguish them based on them.

Offset
^^^^^^

Offset defines the starting point of the job. It is required to shift back in time from the anchor keyframe. The offset can be defined in two ways:

- number of fully-decodable blocks;
- number of seconds.

Number of Blocks
~~~~~~~~~~~~~~~~

.. code-block:: javascript

    {
      "blocks": <int>
    }

Rewinds to the specified number of blocks (keyframes) before the anchor keyframe.

Number of Seconds
~~~~~~~~~~~~~~~~~

.. code-block:: javascript

    {
      "seconds": <float>
    }

Rewinds to the specified number of seconds before the anchor keyframe but always starts from the keyframe.

Job Stop Condition JSON Body
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Last Frame
~~~~~~~~~~

.. code-block:: javascript

    {
      "last_frame": {
        "uuid": <UUID>,
      }
    }

When the next frame UUID is "larger" than the specified, the job will stop. Because the system uses strictly increasing UUIDv7 for frame UUIDs, you can construct a UUIDv7 with the desired timestamp to match the timestamp.

Frame Count
~~~~~~~~~~~

.. code-block:: javascript

    {
      "frame_count": <COUNT>
    }

The job will stop when the specified number of frames is processed.

Keyframe Count
~~~~~~~~~~~~~~

.. code-block:: javascript

    {
      "key_frame_count": <COUNT>
    }

The job will stop when the specified number of keyframes is processed.

Timestamp Delta
~~~~~~~~~~~~~~~

.. code-block:: javascript

    {
      "ts_delta_sec": {
        "max_delta_sec": <float, seconds> // 1.0
      }
    }

The job will stop when the encoded timestamp delta between the last frame and the current frame is larger than the specified value.

Realtime Delta
~~~~~~~~~~~~~~

.. code-block:: javascript

    {
      "real_time_delta_ms": {
        "configured_delta_ms": <int, milliseconds> // 1000
      }
    }

The job will stop when the job live time is larger than the specified value.

Now
~~~

.. code-block:: javascript

    "now"

The job will stop immediately.

Never
~~~~~

.. code-block:: javascript

    "never"

The job will never stop.

Time-synchronized And Fast Jobs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With Replay, you can re-stream with different speed and time synchronization. The system can handle the following cases:

- as-fast-as-possible re-streaming (in most cases it is limited by a receiver);
- time-synchronized re-streaming (sends according to encoded PTS/DTS labels and time corrections);

.. note::

    Regardless of the mode, the system never changes encoded PTS and DTS labels, Replay just re-streams regulating frame delivery.

The mode is defined by the following parameter:

.. code-block:: javascript

    "ts_sync": true

When ``ts_sync`` is set to ``true``, the system will re-stream the video in time-synchronized mode. The system will deliver frames according to the encoded timestamps.

When ``ts_sync`` is set to ``false``, the system will re-stream the video as fast as possible.

Egress FPS Control And Correction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TODO

Routing Labels
^^^^^^^^^^^^^^

TODO

List Job
--------

List the running job matching the given UUID.

.. code-block:: bash

    JOB_UUID=<JOB_UUID> curl http://127.0.0.1:8080/api/v1/job/$JOB_UUID | json_pp

.. list-table::
    :header-rows: 1

    * - Parameter
      - Description
      - Extra
      - Description
    * - Method
      - GET
      - ``/api/v1/job/<jobid>``
      -
    * - Response OK
      - 200 OK
      - ``{...}``
      - see JSON response below
    * - Response Error
      - 500 Internal Server Error
      - ``{"error" => "Reason"}``
      - problem description

JSON Body:

.. code-block:: javascript

    {
       "jobs" : [
          [
             <jobid>,
             { /* job configuration */ },
             { /* job stop condition */}
          ], ...
       ]
    }


List Jobs
---------

List all running jobs.

.. code-block:: bash

    curl http://127.0.0.1:8080/api/v1/job | json_pp

.. list-table::
    :header-rows: 1

    * - Parameter
      - Description
      - Extra
      - Description
    * - Method
      - GET
      - ``/api/v1/job``
      -
    * - Response OK
      - 200 OK
      - ``{...}``
      - see JSON response below
    * - Response Error
      - 500 Internal Server Error
      - ``{"error" => "Reason"}``
      - problem description

JSON Body:

.. code-block:: javascript

    {
       "jobs" : [
          [
             <jobid>,
             { /* job configuration */ },
             { /* job stop condition */}
          ], ...
       ]
    }


List Stopped Jobs
-----------------

List all stopped but not yet evicted jobs.

.. code-block:: bash

    curl http://127.0.0.1:8080/api/v1/job/stopped | json_pp

.. list-table::
    :header-rows: 1

    * - Parameter
      - Description
      - Extra
      - Description
    * - Method
      - GET
      - ``/api/v1/job/stopped``
      -
    * - Response OK
      - 200 OK
      - ``{...}``
      - see JSON response below
    * - Response Error
      - 500 Internal Server Error
      - ``{"error" => "Reason"}``
      - problem description


200 OK JSON Body:

.. code-block:: javascript

    {
       "stopped_jobs" : [
          [
             <jobid>,
             { /* job configuration */ },
             null | "When error, termination reason"
          ], ...
       ]
    }


Delete Job
----------

Forcefully deletes the running job matching the given UUID.

.. code-block:: bash

    JOB_UUID=<JOB_UUID> curl -X DELETE http://127.0.0.1:8080/api/v1/job/$JOB_UUID | json_pp

.. list-table::
    :header-rows: 1

    * - Parameter
      - Description
      - Extra
      - Description
    * - Method
      - DELETE
      - ``/api/v1/job/<jobid>``
      -
    * - Response OK
      - 200 OK
      - ``"ok"``
      - job was deleted
    * - Response Error
      - 500 Internal Server Error
      - ``{"error" => "Reason"}``
      - problem description


Update Job Stop Condition
-------------------------

Updates the stop condition of the running job matching the given UUID.

.. code-block:: bash

    JOB_UUID=<JOB_UUID> curl \
         --header "Content-Type: application/json" -X PATCH \
         --data '{"frame_count": 10000}' \
         http://127.0.0.1:8080/api/v1/job/$JOB_UUID/stop-condition | json_pp

.. list-table::
    :header-rows: 1

    * - Parameter
      - Description
      - Extra
      - Description
    * - Method
      - PATCH
      - ``/api/v1/job/<jobid>/stop-condition``
      -
    * - Request
      - JSON
      - ``{...}``
      - see the `Job Stop Condition JSON Body`_ section in the `Create New Job`_ section
    * - Response OK
      - 200 OK
      - ``"ok"``
      - stop condition was updated
    * - Response Error
      - 500 Internal Server Error
      - ``{"error" => "Reason"}``
      - problem description

