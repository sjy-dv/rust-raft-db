#![warn(clippy::all)]

use rrdb::error::{Error, Result};
use rrdb::storage::debug;
use rrdb::storage::engine::{BitCask, Engine};

fn main() -> Result<()> {
    let args = clap::command!()
        .about("Prints rrdb BitCask contents in human-readable form.")
        .args([clap::Arg::new("file")])
        .get_matches();
    let file: &String = args.get_one("file").unwrap();

    let mut engine = BitCask::new(file.into())?;
    let mut scan = engine.scan(..);
    while let Some((key, value)) = scan.next().transpose()? {
        let (fkey, Some(fvalue)) = debug::format_key_value(&key, &Some(value)) else {
            return Err(Error::Internal(format!("Unexpected missing value at key {:?}", key)));
        };
        println!("{} → {}", fkey, fvalue);
    }
    Ok(())
}