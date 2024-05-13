Introduction
============

When developing computer vision and video analytics pipelines developers often meet challenging problems requiring non-linear stream processing. Such operations may include rewinds, on-demand processing, video footage saving, video storage and retrieval, etc. These problems are hard to solve without specialized storage systems optimized for such tasks.

Replay is a solution for such complex problems. It is a high-performance video storage system that allows developers to store, retrieve, and process video streams in a non-linear fashion. Replay is designed to be used in computer vision and video analytics pipelines. It is optimized for high-throughput video storage and retrieval, and it is capable of handling multiple video streams simultaneously.

Developers can use Replay for various tasks, such as:

- long-term video archive;
- postponed video processing;
- particular video fragment processing;
- video playback in real time, with increased or decreased FPS speed;
- video footage saving;
- selective video processing,
- and many others.

Let us discuss a couple of such use cases in more detail.

Long-Term Video Archive
-----------------------

With Replay you can save your video streams for long term storage simultaneously to its processing. All the discovered metadata is stored as well and in the future you can retrieve required parts for latter processing. The system allows configuring TTL which automatically deletes old data.

Postponed Video Processing
--------------------------

Sometimes you do not want process all the data because it is inefficient, but when the event is triggered to process the video related to it. Replay allows you to store the video stream and process it later when needed. In this use case, there are two pipelines: one for basic analytics and another for more complex processing. The first pipeline processes the video stream in real time and looks for particular events. When such an event is detected the pipeline instructs Replay to send the video fragment related to the event to the second pipeline for more complex processing.

Video Footage Saving
--------------------

Savant has adapters allowing to save video footage to the disk. This is useful when you need to store the video stream for further analysis or for legal purposes. With Replay, when the pipeline detected an event, it can instruct Replay to send the video fragment related to the event to the video file sink.

