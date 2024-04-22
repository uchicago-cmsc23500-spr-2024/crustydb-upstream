use clap::Parser;
use server::{Server, ServerConfig};
/// Entry point for server.
///
/// Waits for user connections and creates a new thread for each connection.
fn main() {
    let config = ServerConfig::parse();
    let mut server = Server::new(config);
    server.run_server();
}
