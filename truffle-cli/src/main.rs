use std::{fs::read_to_string, path::Path};

use clap::Parser;
use rustyline::{DefaultEditor, error::ReadlineError};
use tracing::{error, info};
use truffle::{Simulator, resolve::ResolvedQuery};

#[derive(clap::Parser)]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Validate all of the statements in a SQL file.
    Validate { path: String },
    /// Run a REPL.
    Repl,
}

fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Validate { path } => {
            let sql = read_to_string(path).unwrap();
            let mut sim = Simulator::default();
            if let Err(err) = sim.execute(&sql) {
                info!("{sim:#?}");
                error!("{err}");
            } else {
                info!("{sim:#?}");
                info!("Valid! (syntactically and semantically)");
            }
        }
        Commands::Repl => {
            fn execute_sql(sim: &mut Simulator, sql: &str) -> Option<ResolvedQuery> {
                match sim.execute(sql) {
                    Ok(resolved) => {
                        println!("✅ ok");
                        Some(resolved)
                    }
                    Err(e) => {
                        println!("❌ {e}");
                        None
                    }
                }
            }

            let mut sim = Simulator::default();
            let mut rl = DefaultEditor::new().unwrap();

            println!("truffle repl!");
            println!("type any sql expression and it will tell you if it is valid or not!");
            println!("use .help to see the help menu.");
            loop {
                let readline = rl.readline("truffle >> ");
                match readline {
                    Ok(line) => {
                        if line.starts_with('.') {
                            let mut pieces = line.split_terminator(' ');
                            match pieces.next().unwrap() {
                                ".help" => {
                                    println!("    .tables -> prints the tables");
                                    println!("    .table <TABLE> -> prints table info");
                                    println!("    .constraints <TABLE> -> prints constraints");
                                    println!("    .import <PATH> -> executes file at the path");
                                    println!("    .exit -> exit (can also ctrl+c)");
                                }
                                ".tables" => {
                                    println!(
                                        "{:#?}",
                                        sim.get_tables()
                                            .iter()
                                            .map(|t| t.0)
                                            .collect::<Vec<&String>>()
                                    );
                                }
                                ".table" => {
                                    if let Some(table) = pieces.next()
                                        && let Some(table) = sim.get_table(table)
                                    {
                                        println!("{table:#?}");
                                    } else {
                                        println!("invalid table");
                                    }
                                }
                                ".constraints" => {
                                    if let Some(table) = pieces.next()
                                        && let Some(table) = sim.get_table(table)
                                    {
                                        println!("{:#?}", table.get_all_constraints());
                                    } else {
                                        println!("invalid table for constraints");
                                    }
                                }
                                ".import" => {
                                    if let Some(path) = pieces.next() {
                                        let path = Path::new(path);

                                        if path.is_file() {
                                            let sql = read_to_string(path).unwrap();
                                            execute_sql(&mut sim, &sql);
                                        } else if path.is_dir() {
                                            let dir = path.read_dir().unwrap();
                                            let mut paths = vec![];
                                            for entry in dir {
                                                let entry = entry.unwrap();
                                                let path = entry.path();

                                                if path.is_file() {
                                                    paths.push(path);
                                                }
                                            }

                                            paths.sort();

                                            for path in paths {
                                                let sql = read_to_string(path).unwrap();
                                                execute_sql(&mut sim, &sql);
                                            }
                                        }
                                    } else {
                                        println!("invalid path for importing");
                                    }
                                }
                                ".exit" => {
                                    break;
                                }
                                _ => {
                                    println!("unknown command: {line}");
                                }
                            }

                            continue;
                        }

                        if let Some(resolve) = execute_sql(&mut sim, &line) {
                            println!("{resolve}")
                        }
                    }
                    Err(ReadlineError::Interrupted) => {
                        println!("CTRL-C");
                        break;
                    }
                    Err(ReadlineError::Eof) => {
                        println!("CTRL-D");
                        break;
                    }
                    Err(err) => {
                        println!("Error: {err:?}");
                        break;
                    }
                }
            }
        }
    }
}
