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
