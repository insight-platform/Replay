# Replay

Replay is Savant ecosystem video storage and on-demand video playback service.

## What It Provides

Replay is advanced service providing following features:

- collects video from multiple streams (archiving with TTL eviction);
- provides a REST API for video retrieval to Savant sinks or modules;
- can work as a sidecar or intermediary service in Savant pipelines;
- PTS-synchronized and fast video retrieval;
- supports setting minimum and maximum PTS to increase or decrease the video playback speed;
- supports configurable video retrieval stop conditions;
- can fix incorrect PTS in video streams;
- can remap video streams to other source IDs;
- can set routing labels for video streams (future feature);
- can look backward when video stream retrieval;
- can set additional attributes to retrieved video streams;

## How It Is Implemented

Replay is high-performance service implemented in Rust programming language. It uses RocksDB as a storage engine to
deliver video with low latency and high throughput.

## How One Can Use It

**Online mode**: when the pipeline finds a key event it can request video from Replay to another module to implement
advanced analysis or to a sink to save it somehow;

**Offline mode**: when the user needs particular video to be processed one more time, it can request it from Replay
based on Metadata information stored in a 3rd-party storage;

## LICENSE

Replay is licensed under the BSL-1.1 license. See [LICENSE](LICENSE) for more information.

### Obtaining Production-Use License

To obtain a production-use license, please fill out the form
at [In-Sight Licensing](https://forms.gle/kstX7BrgzqrSLCJ18).