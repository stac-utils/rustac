use clap::Parser;
use rustac::Rustac;

#[tokio::main]
async fn main() {
    let args = Rustac::parse();
    std::process::exit(match args.run(true).await {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("ERROR: {err}");
            1 // TODO make this more meaningful
        }
    })
}
