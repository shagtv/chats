use chats_stress::cli::Args;
use chats_stress::run::run;

fn main() {
    let args = match Args::parse(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.gen_threads)
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(run(args)) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("chats-stress: {e}");
            std::process::exit(1);
        }
    }
}
