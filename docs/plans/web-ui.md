
# OrbWeaver UI — Frontend brief

Competitive/reference notes for a front-end developer. Focus: **routable views, components, controls**. Analytics/DSP out of scope.

## Product shells

| Shell | Suggested route | Role |
| --- | --- | --- |
| Web signal analyzer | `/analyzer` | Spectrum + waterfall triage (focus on survey) |
| Web Rx/Tx | `/ham` | Net operations for amature bands (RX + TX) similar to SDR++ |
| Web DVR | `/dvr` | Remote replay, routing, detector config |
| Intercepts| `/intercepts`| Essentially a view over a lanceDB instance of capture recordings
| Sensor overview | `/sensors` | Map of node positions |
| GeoServer | `/geo` | Map / TDOA–FDOA geolocation (requires 2+ RX) |
| Agent | `/herdr` | Pass through to herdr webGUI for background task orchestration |
| HexEditor | `/hex` | Map / TDOA–FDOA geolocation (requires 2+ RX) |
| Synth | `/synth` | Synthetic, Augmented data generator |

Note - heavy use of HTMLCanvas, WebGL, WebSockets, Wasm, and serverside rendering likely required
to achieve competitive performance with C++/Qt desktop applications

## Shared chrome
- **AppBar:** logo, live / record / offline, backend health 
- **LeftNav:** receivers, detectors, DVR, intercepts, map, plugins, settings
- **StatusBar:** IBW, center freq, sample rate, disk, C2 health
- **RightRail (optional):** flags/markers 

## Native workspace (`/analyzer`)

| Region | Component | Notes |
| --- | --- | --- |
| Top | `SpectrumPlot` | PSD, shared freq axis |
| Mid | `WaterfallPlot` | Time × freq, detection boxes (server should provide detection boxes) |
| Bottom (tabs) | `InterceptTable` \| `IqPane` \| `BitBreakout` | Operator picks |

Loop: waterfall energy → `DetectionBox` → intercept row → audio / bit breakout / map.

## Components

- Plots: `SpectrumPlot`, `WaterfallPlot`, `WaveformPlot`, `ConstellationPlot`, `BitBreakout` (hex + bit raster)
- Overlays: `FreqPin` (green flags on freq axis), `DetectionBox`, `ChannelBar`, `MaskOverlay`, `AnnotationLabel`, `AllocationOverlay`
- Data: `InterceptTable`, `AttachmentChip`, `SensorList` / `ReceiverCard`, `DetectorList`, `MeterStack`
- Web: `CitadelNetworkView`, `TaskingPanel`, `DvrTimeline`
- FHSS: `DemodulatedOscilliscope`, `DemodRaster` 

`FreqPin` ≠ detection boxes.

## Controls (operator, not DSP)

- Global: play/pause, record, center/span/IBW/RBW, ref level, colormap, receiver select
- Plots: zoom/pan, click to tune or pin, drag box to zoom or ad-hoc detector, click box → table row
- Detectors: toggle, threshold; OmniSIG start/stop/config
- Remote: detector CRUD on web DVR; Citadel tasking over websocket

## IA
App
├── AppBar
├── LeftNav
├── Main
│ ├── /workspace
│ ├── /analyzer
│ ├── /dvr
│ ├── /intercepts
│ ├── /sensors
│ ├── /citadel
│ └── /geo
├── RightRail
└── StatusBar
