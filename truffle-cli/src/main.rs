use std::fs::read_to_string;

use clap::Parser;
use rustyline::{DefaultEditor, error::ReadlineError};
use tracing::{error, info};
use truffle_sim::{GenericDialect, Simulator};

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
            let mut sim = Simulator::new(Box::new(GenericDialect {}));
            if let Err(err) = sim.execute(&sql) {
                info!("{sim:#?}");
                error!("{err}");
            } else {
                info!("{sim:#?}");
                info!("Valid! (syntactically and semantically)");
            }
        }
        Commands::Repl => {
            let mut sim = Simulator::new(Box::new(GenericDialect {}));
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
                                    println!("    .constraints <TABLE> -> prints constraints");
                                    println!("    .import <PATH> -> executes file at the path");
                                    println!("    .exit -> exit (can also ctrl+c)");
                                }
                                ".tables" => {
                                    println!("{:#?}", sim.get_tables());
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
                                    if let Some(path) = pieces.next()
                                        && let Ok(sql) = read_to_string(path)
                                    {
                                        match sim.execute(&sql) {
                                            Ok(_) => {
                                                println!("✅ ok");
                                            }
                                            Err(e) => {
                                                println!("❌ {e}");
                                            }
                                        };
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

                        match sim.execute(&line) {
                            Ok(_) => {
                                println!("✅ ok");
                            }
                            Err(e) => {
                                println!("❌ {e}");
                            }
                        };
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
