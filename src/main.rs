use clap::Parser;
use cli_table::Table;
use cli_table::{print_stdout, WithTitle};
use serde::Deserialize;
use serde_json::Value;
use std::str::from_utf8;
use std::{collections::HashMap, fs::File, io::BufReader, process::exit};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProfileJSON {
    pub start: i64,
    pub end: i64,
    pub dremio_version: String,
    pub json_plan: String,
    pub command_pool_wait_millis: i64,
    pub query: String,
    pub user: String,
    pub state: i16,
}

#[derive(Table)]
struct TopLineSummary {
    #[table(title = "Field")]
    pub field: String,
    #[table(title = "Value")]
    pub value: String,
    #[table(title = "Explain")]
    pub explain: String,
}

#[derive(Table)]
struct FilterCostSummary {
    #[table(title = "Phase-Thread")]
    pub phase_thread: String,
    #[table(title = "Type")]
    pub type_name: String,
    #[table(title = "Filter")]
    pub filter: String,
    #[table(title = "Row Count")]
    pub rows: String,
    #[table(title = "Total Cost")]
    pub total_cost: String,
}

fn main() {
    let args = Cli::parse();
    let file = File::open(args.path);
    let file_ref = file.unwrap_or_else(|error| {
        println!("unable to open file due to error: {error}");
        exit(1);
    });
    let reader = BufReader::new(file_ref);
    let json_result = serde_json::from_reader(reader);
    let profile: ProfileJSON = json_result.unwrap_or_else(|error| {
        println!("unable to deserialize json due to error: {error}");
        exit(1);
    });

    let plan_json = profile.json_plan;
    let escaped = escape(&plan_json);
    let escaped_str = escaped.as_str();
    let plan_result = serde_json::from_str(escaped_str);
    let plan: Value = plan_result.unwrap_or_else(|error| {
        println!("unable to deserialize plan json due to error: {error}");
        exit(1);
    });

    let cost_summary: Vec<FilterCostSummary> = plan
        .as_object()
        .unwrap()
        .iter()
        .filter(|x| {
            let p = x.1.as_object().unwrap();
            let obj = p["\"values\""].as_object().unwrap();
            obj.contains_key("\"condition\"")
        })
        .map(|x| {
            let p = x.1.as_object().unwrap();

            let op = p.get("\"op\"").unwrap();
            let row_count_raw = p.get("\"rowCount\"").unwrap();
            let row_count: i64 = row_count_raw.as_f64().unwrap() as i64;
            let cumulative_cost = p.get("\"cumulativeCost\"").unwrap();
            let cumulative_cost_str = cumulative_cost.as_str();

            let obj = p.get("\"values\"").unwrap();
            let condition = obj.get("\"condition\"").unwrap();
            let phase_thread = x.0;
            let filter = FilterCostSummary {
                phase_thread: phase_thread.to_string(),
                type_name: op.to_string(),
                filter: condition.to_string(),
                rows: thousands(row_count),
                total_cost: thousands(total_cost(cumulative_cost_str.unwrap())),
            };
            filter
        })
        .collect();
    assert!(print_stdout(cost_summary.with_title()).is_ok());
}

fn thousands(i: i64) -> String {
    i.to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .unwrap()
        .join(",")
}
fn total_cost(s: &str) -> i64 {
    let mut chars = s.chars();
    chars.next();
    chars.next_back();
    let trimmed = chars.as_str();
    let tokens: Vec<&str> = trimmed.split(", ").collect();
    let vec_floats: Vec<f64> = tokens
        .iter()
        .map(|e| {
            let t: Vec<&str> = e.split(" ").collect();
            let value = t[0];
            let f: f64 = value.parse().unwrap();
            f
        })
        .collect();

    let total_cost: f64 = vec_floats.into_iter().reduce(|acc, e| acc + e).unwrap();
    total_cost as i64
}

fn escape(s: &String) -> String {
    s.replace("\\n", "\n")
}

#[derive(Parser)]
struct Cli {
    // The path to the profile
    path: std::path::PathBuf,
}
