use clap::Parser;
use cli_crusty::{Client, ClientConfig};

fn main() {
    let config = ClientConfig::parse();
    let mut client = Client::new(config);
    client.run_cli();
}
