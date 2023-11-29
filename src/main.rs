use std::env::var;
use std::f32::consts::E;
extern crate dotenv;
use csv;
use dotenv::dotenv;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::sql::Object;
use surrealdb::{Response, Surreal};
use tokio;
use walkdir::{DirEntry, WalkDir};
extern crate serde_json;

use clap::Parser;
use std::collections::HashMap;
use std::error::Error;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use surrealdb::sql::Thing;

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Parser)]
struct Cli {
    command: Option<String>,
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let _args = Cli::parse();
    let db = Surreal::new::<Ws>(var("SURREALDB_ADDRESS").unwrap().as_str())
        .await
        .unwrap();

    db.signin(Root {
        username: var("SURREALDB_USER").unwrap().as_str(),
        password: var("SURREALDB_PASS").unwrap().as_str(),
    })
    .await
    .unwrap();

    db.use_ns(var("SURREALDB_NAMESPACE").unwrap().as_str())
        .use_db(var("SURREALDB_DATABASE").unwrap().as_str())
        .await
        .unwrap();
    load_datas(&db).await.unwrap();
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SurelloSourceType {
    Surql,
    Csv,
    Parquet,
}

#[derive(Debug, Serialize, Deserialize)]
struct SurelloHistoryEntry {
    source_path: String,
    source_type: SurelloSourceType,
    execution_datetime_utc: String,
    execution_result: String,
}

#[derive(Debug, Deserialize)]
struct Record {
    #[allow(dead_code)]
    id: Thing,
}

pub async fn load_csv(db_client: &Surreal<Client>, path: &Path) -> Result<(), Box<dyn Error>> {
    // This introduces a type alias so that we can conveniently reference our
    // record type.
    type Record = HashMap<String, String>;

    let mut rdr = csv::Reader::from_path(path)?;
    for result in rdr.deserialize() {
        let record: Record = result?;
        println!("{:?}", record);
    }
    Ok(())
}
fn determine_target(path: &Path) -> Option<SurelloSourceType> {
    let extension = path.extension().unwrap().to_str().unwrap();
    match extension {
        "surql" => Some(SurelloSourceType::Surql),
        "csv" => Some(SurelloSourceType::Csv),
        _ => None,
    }
}

async fn register_as_done(
    db_client: &Surreal<Client>,
    source_path: &Path,
    source_type: SurelloSourceType,
    response: &str,
) -> Result<(), surrealdb::Error> {
    db_client
        .create("surello_history")
        .content(SurelloHistoryEntry {
            source_path: source_path.to_str().unwrap().to_string(),
            source_type: source_type,
            execution_datetime_utc: chrono::Utc::now().to_rfc3339(),
            execution_result: response.to_string(),
        })
        .await
        .map(|_: Vec<Record>| ())
}

async fn load_surql(
    db_client: &Surreal<Client>,
    source_path: &Path,
) -> Result<(), surrealdb::Error> {
    let statements: String = fs::read_to_string(source_path).unwrap();

    println!("Executing {}", source_path.display());
    let response = db_client.query(statements).await;
    match response {
        Ok(response) => {
            println!("Response: {:?}", response);
            register_as_done(db_client, source_path, SurelloSourceType::Surql, "ok").await
        }
        Err(e) => Err(e),
    }
}

pub async fn load_datas(db_client: &Surreal<Client>) -> Result<(), surrealdb::Error> {
    //Go through the tables in db folder and create them in the database
    // recursively walk directory db for all .rusql files
    // for each file, create a table with the name of the file
    // and insert the contents of the file into the table
    let walker = WalkDir::new("surello_data").into_iter();
    let surello_history: Vec<SurelloHistoryEntry> =
        db_client.select("surello_history").await.unwrap();
    for entry in walker {
        let dir_entry = entry.unwrap();
        if dir_entry.file_type().is_file() {
            let source_path = dir_entry.path();
            let maybe_target = determine_target(source_path);

            match maybe_target {
                Some(source_type) => {
                    let historic_execution = surello_history.iter().find(|history_entry| {
                        history_entry.source_path == source_path.display().to_string()
                            && history_entry.source_type == source_type
                    });
                    match historic_execution {
                        Some(execution) => {
                            println!(
                                "Skipping {} because it was previously executed at {}",
                                dir_entry.path().display(),
                                execution.execution_datetime_utc
                            );
                            continue;
                        }
                        None => match source_type {
                            SurelloSourceType::Surql => {
                                load_surql(&db_client, source_path).await;
                                ()
                            }
                            SurelloSourceType::Csv => {
                                load_csv(&db_client, source_path).await;
                                ()
                            }
                            SurelloSourceType::Parquet => todo!(),
                        },
                    }
                }
                None => println!(
                    "Unsupported type or unknown file: {}",
                    dir_entry.path().display()
                ),
            }
        }
    }

    Ok(())
}
