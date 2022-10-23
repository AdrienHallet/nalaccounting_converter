use std::{
    fs::{File},
    io::{self, BufRead},
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
};

use chrono::{NaiveDate, ParseError, Datelike};
use serde_json::{json, Value};

fn main() {
    let mut transactions: Vec<Transaction> = vec![];
    match read_lines("./input.tsv") {
        Ok(lines) => {
            // Consumes the iterator, returns an (Optional) String
            for line in lines {
                match line {
                    Ok(ip) => {
                         match parse_transaction(ip) {
                            Ok(transaction) => {
                                transactions.push(transaction);
                            },
                            Err(parse_error) => println!("ERROR: {:?}", parse_error),
                        };
                    }
                    Err(_) => todo!(),
                }
            }
        }
        Err(e) => {
            panic!("{}", e.to_string())
        }
    }
    println!("Parsed {} transactions", transactions.len());
    let mut json = get_dexie_structure();
    json["data"]["tables"][0]["rowCount"] = json!(transactions.len());
    let val = json["data"]["data"][0]["rows"].as_array_mut().unwrap();
    for transaction in transactions {
        if transaction.is_transfert() {
            continue;
        }
        val.push(transaction.to_value());
    }
    println!("{}", json)
}

fn get_dexie_structure() -> serde_json::Value {
    json!({
        "formatName": "dexie",
        "formatVersion": 1,
        "data": {
            "databaseName": "budjet",
            "databaseVersion": 1,
            "tables": [
                {
                    "name": "transactions",
                    "schema": "++id,amount,title",
                    "rowCount": 0
                }
            ],
            "data": [
                {
                    "tableName": "transactions",
                    "inbound": true,
                    "rows": []
                }
            ],
        }
    })
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

// Constants of filestructure declaration
const DATE_FORMAT: &str = "%d/%m/%Y";
const DATE: usize = 0;
const CATEGORY: usize = 1;
const DESCRIPTION: usize = 4;
const EXPENSE: usize = 11;
const INCOME: usize = 12;
static SEQUENCE: AtomicU32 = AtomicU32::new(1);

// Parsing
#[derive(Debug)]
struct Transaction {
    id: u32,
    date: NaiveDate,
    expense: Option<f32>,
    income: Option<f32>,
    description: String,
    category: String,
}

impl Transaction {
    fn to_value(&self) -> Value {
        json!({
            "date": format!("{}-{}-{}", &self.date.year(), &self.date.month(), &self.date.day()),
            "title": format!("{}", &self.description),
            "amount": format!("{}", &self.amount()),
            "id": format!("{}", &self.id),
        })
    }

    fn amount(&self) -> &f32 {
        match (&self.expense, &self.income) {
            (Some(exp_value), None) => exp_value,
            (None, Some(inc_value)) => inc_value,
            _ => panic!("Unknown amount pattern {:?}", &self)
        }
    }

    fn is_transfert(&self) -> bool {
        matches!(self.category.as_str(), "Transfert")
    }
}

fn parse_transaction(raw_transaction: String) -> Result<Transaction, ParseError> {
    let split_transaction = raw_transaction.split('\t').collect::<Vec<&str>>();
    let date = NaiveDate::parse_from_str(split_transaction[DATE], DATE_FORMAT)?;
    let expense = split_transaction[EXPENSE].replace(',', ".").parse::<f32>().ok();
    let income = split_transaction[INCOME].replace(',', ".").parse::<f32>().ok();
    let description = split_transaction[DESCRIPTION].to_string();
    let category = split_transaction[CATEGORY].to_string();

    Ok(Transaction {
        id: SEQUENCE.fetch_add(1, Ordering::SeqCst),
        date,
        expense,
        income,
        description,
        category
    })
}
