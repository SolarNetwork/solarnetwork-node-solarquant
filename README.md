# SolarQuant Plugin

SolarNode plugin that buffers datums from the datum pipeline and forwards them to a SolarQuant HTTP service via `POST /measure`. Predictions returned by the service are posted back to SolarNetwork as new datums.

Implements the [SolarQuant plugin schema](https://github.com/ecosuite/solarquant-plugin). Any container exposing `/measure` and `/health` works.

## Building

Requires Java 17, Apache Ant, and Git LFS. Clone [solarnetwork-build](https://github.com/SolarNetwork/solarnetwork-build) as a sibling directory and run `git lfs pull` in it.

```
parent/
  solarnetwork-build/
  solarquant-solarnetwork-node/
    net.solarnetwork.node.datum.solarquant/
```

```bash
cd solarquant-solarnetwork-node/net.solarnetwork.node.datum.solarquant
ant jar
```

Output: `net.solarnetwork.node.datum.solarquant/target/net.solarnetwork.node.datum.solarquant-1.0.0.jar`

## Configuration

Add a **SolarQuant Anomaly Detection** component in the SolarNode Settings UI.

| Setting | Default | Description |
|---------|---------|-------------|
| Docker Image |  | Image to pull and run. Leave empty to use an existing service. |
| Service URL | `http://localhost:8000` | Base URL of the SolarQuant service. Auto-set when Docker Image is configured. |
| Source ID Filter |  | Regex matching source IDs to forward. |
| Upload Source ID |  | Base source ID for predictions. `sourceIndex` is appended (e.g. `/solarquant/1`). |
| Flush Interval | `60` | How often buffered datums are sent, in seconds. |
| Docker Command | `bin/solarquant` | Path to the Docker management script. |
| HTTP Customizer |  | Service Name of an `HttpRequestCustomizerService` to apply to outbound requests (e.g. to add authentication). |
| Manual Node ID |  | The node ID to use when forwarding datum to the service, instead of SolarNode's actual node ID. |
| Source ID Mappings |  | An optional list of search/replace mappings to apply to datum source IDs before forwarding to the service.  See [below](#source-id-mappings) for more detail. |

### Source ID Mappings

The **Source ID Mappings** list can be configured with any number of search regular
expressions with associated replacement templates. Use the <kbd>+</kbd> and <kbd>-</kbd> buttons
to add/remove mappings.

| Setting | Description |
|:--------|:------------|
| Source Match | The datum source ID pattern to replace. Capture groups can be referenced in **Replacement**. |
| Replacement  | The source ID replacement template to use when Source Match matches a datum source ID. |

Use regular expression capture groups to extract parts of a matching datum source ID to use in the
replacement source ID value. The replacement template can refer to the entire matched source ID with
the `{s}` placeholder, or capture groups like `{n}` where `n` is the capture group index, starting
from 1.

For example, a **Source Match** `/x/(d+)` with **Replacement** `/y/{1}` would map an input source ID
`/x/123` to `/y/123`.



The plugin calls an external script to manage containers. Install `net.solarnetwork.node.datum.solarquant/def/solarquant.sh` to the SolarNode bin directory -- see `net.solarnetwork.node.datum.solarquant/def/README.md`.

## Protocol

See the [SolarQuant plugin schema](https://github.com/ecosuite/solarquant-plugin) for the full OpenAPI spec.

`POST /measure` -- send datums, receive predictions. `GET /health` -- liveness check (shown in SolarNode dashboard).

Datum fields map to SolarNetwork sample types: `i` (instantaneous), `a` (accumulating), `s` (status).

## Reference

- [SolarQuant plugin schema](https://github.com/ecosuite/solarquant-plugin) -- OpenAPI spec and container contract

## License

Apache License 2.0
