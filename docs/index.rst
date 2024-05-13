Replay Documentation
====================

Replay is Savant ecosystem video storage and on-demand video playback service providing following features:

* collects video from multiple streams (archiving with TTL eviction);
* provides a REST API for video retrieval to Savant sinks or modules;
* can work as a sidecar or intermediary service in Savant pipelines;
* PTS-synchronized and fast video retrieval;
* supports setting minimum and maximum PTS to increase or decrease the video playback speed;
* supports configurable video retrieval stop conditions;
* can fix incorrect PTS in video streams;
* can remap video streams to other source IDs;
* can set routing labels for video streams (future feature);
* can look backward when video stream retrieval;
* can set additional attributes to retrieved video streams;

.. toctree::
   :maxdepth: 2
   :caption: Contents

   sections/introduction
