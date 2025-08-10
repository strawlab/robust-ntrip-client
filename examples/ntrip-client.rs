#[tokio::main(flavor = "current_thread")]
async fn main() -> eyre::Result<()> {
    if std::env::var_os("RUST_LOG").is_none() {
        // SAFETY: We ensure that this only happens in single-threaded code
        // because this is immediately at the start of main() and no other
        // threads have started.
        unsafe { std::env::set_var("RUST_LOG", "info") };
    }
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        println!("Usage: ntrip-client NTRIP_URL");
        eyre::bail!("Invalid command line arguments");
    }

    let opts = robust_ntrip_client::RobustNtripClientOptions {
        timeout: Some(std::time::Duration::from_secs(5)),
        ..Default::default()
    };
    let ntrip = robust_ntrip_client::RobustNtripClient::new(&args[1], opts).await?;
    let mut ntrip = robust_ntrip_client::ParsingNtripClient::new(ntrip);

    tracing::info!("entering main loop");
    loop {
        let msg = ntrip.next().await?;
        tracing::info!(
            "message {}: {} bytes",
            msg.message_number(),
            msg.frame_data().len()
        );
    }
}
