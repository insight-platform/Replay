# Replay

Collect video streams and re-stream them effortlessly with REST API.

![](assets/replay_usage_diagram.png)

## Features

Replay is an advanced storage providing features required for non-linear computer vision and video analytics:

- collects video from multiple streams (archiving with TTL eviction);
- provides a REST API for video re-streaming to Savant sinks or modules;
- supports time-synchronized and fast video re-streaming;
- supports configurable video re-streaming stop conditions;
- supports setting minimum and maximum frame duration to increase or decrease the video playback speed;
- can fix incorrect TS in re-streaming video streams;
- can look backward when video stream re-streamed;
- can set additional attributes to retrieved video streams;
- can work as a sidecar or intermediary service in Savant pipelines.

## How It Is Implemented

Replay is implemented with Rust and RocksDB. It allows delivering video with low latency and high
throughput. Replay can be deployed on edge devices and in the cloud on ARM or X86 devices.

## Documentation

The documentation is available at [GitHub Pages](https://insight-platform.github.io/Replay/).

## License

Replay is licensed under the BSL-1.1 license. See [LICENSE](LICENSE) for more information.

### Obtaining Production-Use License

To obtain a production-use license, please fill out the form
at [In-Sight Licensing](https://forms.gle/kstX7BrgzqrSLCJ18).