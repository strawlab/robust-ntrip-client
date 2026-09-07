[![Crates.io](https://img.shields.io/crates/v/robust-ntrip-client.svg)](https://crates.io/crates/robust-ntrip-client)
[![Documentation](https://docs.rs/robust-ntrip-client/badge.svg)](https://docs.rs/robust-ntrip-client/)
[![Crate License](https://img.shields.io/crates/l/robust-ntrip-client.svg)](https://crates.io/crates/robust-ntrip-client)

# robust-ntrip-client

Rust client library to connect to Network Transport of RTCM via Internet
Protocol (NTRIP) server. (RTCM stands for Radio Technical Commission for
Maritime services and is the message type carrying GNSS correction signals to
enable centimeter-resolution GNSS position finding.)

See also the [`ntrip-client` crate](https://crates.io/crates/ntrip-client). I
was unaware of this other crate at the time I began writing
`robust-ntrip-client`.

In addition to the `robust-ntrip-client` crate, this repository contains an
example program which may be interesting.

## Reconnection policy: we follow QGroundControl

RTCM 10410.1, the NTRIP 2.0 standard, is paywalled, and RTCM's free guidance
paper for client developers ("NTRIP Client Devices / Best Practices",
2023-SC104-1344) stops short of saying which failures to retry. Surveying
shipping clients found no agreement either: pygnssutils treats every HTTP error
as final, gpsd fails the attempt and leaves reconnection to its caller, and
QGroundControl retries all but one status.

Rather than invent a fifth policy, this crate copies QGroundControl's, on the
grounds that QGC does the same job — streaming RTCM corrections to a drone
autopilot — and that matching one real implementation exactly is easier to defend
and to re-check. Specifically, from **QGroundControl v5.0.3-1265-gf1883f93a**
(`f1883f93a5ba0b5e9c1ec88b3815b290e2608697`, 2026-08-12):

| Behaviour | Ours | QGC |
| --- | --- | --- |
| HTTP 401 | returned to the caller, never retried | `NTRIPHttpTransport.cc:388-392` maps it to `AuthFailed`, which `isRetryable()` refuses (`NTRIPManager.cc:82-91`) |
| Any other non-2xx status, 404 included | retried | `NTRIPHttpTransport.cc:406` maps it to `HttpError`, which is retryable |
| Transport errors (DNS, refused, reset, timeout) | retried | retryable by `isRetryable()`'s default arm |
| Backoff | 1, 2, 4, 8, 16, 30, 30, ... seconds | `kMinReconnectMs * (1 << qMin(attempts, 5))` capped at `kMaxReconnectMs`, `NTRIPManager.cc:367-370` and `NTRIPManager.h:159-161` |
| Giving up | after 100 attempts **that reached the caster** — see below | `kMaxReconnectAttempts`, `NTRIPManager.h:161` |
| `Ntrip-Version` | `Ntrip/2.0` | `NTRIPHttpTransport.cc:77` |
| The caster's error reply body | captured on `CasterHttpError`, tidied the same way for display | `NTRIPHttpTransport.cc:394-406` |
| Resetting the ladder | **on data arriving, not on handshake** — see below | `NTRIPManager.cc:326` |

`max_backoff_duration` plays QGC's `kMaxReconnectMs` and defaults to the same 30
seconds; the other two values are constants, as they are in QGC. The revision and
every file:line above are repeated in comments next to the code that mirrors
them, so the values can be cross-checked against a QGC checkout.

Two deliberate departures, both because QGC's caller is an operator watching a
GUI and ours is unattended software.

Failures that never reached the caster — no route, DNS down, connection refused:
what an uplink that is not up yet looks like — are retried indefinitely rather
than counting towards the limit. QGC can afford to stop, because stopping puts a
message in front of someone who is sitting there. A rig that boots before its
uplink comes up should be streaming corrections an hour later, not holding an
error nobody read. The limit still applies to attempts the caster answered, which
are the ones where something may need correcting and where RTCM warns about
hammering.

Second, QGC resets its attempt counter when the HTTP handshake
completes (`NTRIPManager.cc:326`) rather than when data arrives, so a caster
which accepts a connection and then stays silent is retried at the minimum
interval forever — the hammering RTCM's guidance warns gets a client banned. We
reset on data instead, so a silent caster climbs the same ladder as a refused one
and eventually exhausts the attempt limit. Where the reset sits looks like an
artefact of QGC's state machine rather than a considered choice.

A caller that needs the caster's own explanation can recover it by downcasting
the returned report to `CasterHttpError`, which carries the status and the reply
body. RTCM's guidance paper asks for that text to reach the user, and a 404
saying "mountpoint temporarily offline" is a different operational problem from
one saying "no such mountpoint".

## Example usage
```rust
#[tokio::main]
async fn main() -> eyre::Result<()> {
   let raw_client = robust_ntrip_client::RobustNtripClient::new(
       "ntrip://username:password@example-ntrip-server.com/mountpoint",
       Default::default()
   ).await?;
   let mut ntrip = robust_ntrip_client::ParsingNtripClient::new(raw_client);

   loop {
       let msg = ntrip.next().await?;
       println!(
           "message {}: {} bytes",
           msg.message_number(),
           msg.frame_data().len()
       );
   }
}
```

## Example binary: ntrip-client

### What is it

 - CLI program to listen to NTRIP data source.

### How to run

```bash
cargo run --example ntrip-client -- NTRIP_URL
```

Where `NTRIP_URL` would be something like `ntrip://username:password@host[:port]/mountpoint`.

## License

MIT OR Apache-2.0, at your choice.

## Funding

Funded by the Deutsche Forschungsgemeinschaft (DFG) [project
543356743](https://gepris.dfg.de/gepris/projekt/543356743?language=en),
"Enhancing Insect Tracking Precision from Drones: Fusing Multiple Sensor Data to
Estimate Insect Position with Quantified Uncertainty" awarded to Andrew Straw as
part of [SPP 2433: Metrology on flying
platforms](https://www.uni-bremen.de/en/spp-2433).
