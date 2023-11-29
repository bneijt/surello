use std::env::var;
use std::f32::consts::E;
extern crate dotenv;

use dotenv::dotenv;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::sql::Object;
use surrealdb::{Response, Surreal};
use tokio;
use walkdir::{DirEntry, WalkDir};

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

#[derive(Debug, Serialize, Deserialize)]
struct SurelloHistoryEntry {
    source_name: String,
    source_type: String,
    execution_datetime_utc: String,
    execution_result: String,
}

#[derive(Debug, Deserialize)]
struct Record {
    #[allow(dead_code)]
    id: Thing,
}

pub async fn load_datas(db_client: &Surreal<Client>) -> Result<(), Box<dyn Error>> {
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
            if dir_entry.file_name().to_str().unwrap().ends_with(".surql") {
                println!("Found {}", dir_entry.path().display());
                let source_name = dir_entry.file_name().to_str().unwrap();
                let source_type = "surql";

                //Check if this source_name, source_type is already in the surrello_history table
                let historic_execution = surello_history.iter().find(|history_entry| {
                    history_entry.source_name == source_name
                        && history_entry.source_type == source_type
                });
                match historic_execution {
                    Some(execution  ) => {
                        println!("Skipping {} {} because it was previously executed at {}", source_name, source_type, execution.execution_datetime_utc);
                        continue;
                    }
                    None => {}
                    
                }

                let statements: String = fs::read_to_string(dir_entry.path())?;

                println!("Executing {} {}", source_name, source_type);
                let response = db_client.query(statements).await;
                match response {
                    Ok(response) => {
                        println!("Response: {:?}", response);
                        let _created: Vec<Record> = db_client
                            .create("surello_history")
                            .content(SurelloHistoryEntry {
                                source_name: source_name.to_string(),
                                source_type: source_type.to_string(),
                                execution_datetime_utc: chrono::Utc::now().to_rfc3339(),
                                execution_result: format!("{:?}", response).to_string(),
                            })
                            .await
                            .unwrap();
                        // dbg!(&created);
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                        // db_client
                        //     .create(("surello_history", source_name))
                        //     .content(SurelloHistoryEntry {
                        //         source_name,
                        //         source_type,
                        //         execution_datetime_utc: "execution_datetime_utc",
                        //         execution_status: false,
                        //     })
                        //     .await
                        //     .unwrap();
                    }
                }
            }
        }
    }

    Ok(())
}
