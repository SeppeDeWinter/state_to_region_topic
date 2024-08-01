use std::collections::HashMap;
use std::io::{self, BufRead};
use std::sync::Arc;
use std::fs::File;
use std::path::Path;
use clap::Parser;
use flate2::read::GzDecoder;
use arrow;
use parquet;

#[derive(Debug, Clone)]
struct UnableToParseError;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    input: String,
    #[arg(short, long)]
    output: String
}


#[derive(Hash, Eq, PartialEq, Debug)]
struct StateEntry {
    region_id: u64,
    topic_id: u64
}

impl StateEntry {
    fn new(region_id: u64, topic_id: u64) -> StateEntry {
        StateEntry {region_id, topic_id}
    }
    fn new_from_str(s: &str) -> Option<StateEntry> {
        let v: Vec<&str> = s.split_whitespace().collect();
        if v.len() != 6 {
            return None
        }
        let region_id:u64 = v[4].parse::<u64>().ok()?;
        let topic_id: u64 = v[5].parse::<u64>().ok()?;

        Some(StateEntry::new(region_id, topic_id))
    }
}

fn create_reader<P>(filename: P) -> io::Result<io::BufReader<GzDecoder<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    let d = GzDecoder::new(file);
    Ok(io::BufReader::new(d))
}

fn update_occurence(line: &str, occurence: &mut HashMap<StateEntry, u64>) -> Result<(), UnableToParseError>  {
    match StateEntry::new_from_str(line) {
        Some(entry) => {
            occurence.entry(entry).and_modify(|counter| *counter += 1).or_insert(1);
            Ok(())
        },
        None => {
            Err(UnableToParseError)
        }
    }
}

fn occurence_to_recordbatch(occurence: &HashMap<StateEntry, u64>) -> arrow::array::RecordBatch {
    let mut region_ids: Vec<u64> = Vec::new();
    let mut topic_ids: Vec<u64> = Vec::new();
    let mut values:Vec<u64> = Vec::new();
    
    for (key, value) in occurence {
        region_ids.push(key.region_id);
        topic_ids.push(key.topic_id);
        values.push(*value);
    }

    println!("{} entries", region_ids.len());

    let schema = arrow::datatypes::Schema::new(
        vec![
            arrow::datatypes::Field::new("region_id", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("topic_id",  arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("occurence", arrow::datatypes::DataType::UInt64, false)
        ]
    );
    arrow::array::RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(arrow::array::UInt64Array::from(region_ids)),
            Arc::new(arrow::array::UInt64Array::from(topic_ids)),
            Arc::new(arrow::array::UInt64Array::from(values))
        ]
    ).unwrap()
}

fn main() {
    let args = Args::parse();
    let mut reader = create_reader(args.input).unwrap();
    let mut line = String::with_capacity(50);
    //let mut i: usize = 0;
    let mut occurence: HashMap<StateEntry, u64> = HashMap::new();
    while reader.read_line(&mut line).unwrap_or(0) > 0 {
        //i = i + 1;
        match update_occurence(&line, &mut occurence) {
            Err(_) => {println!("Unable to parse: {}", &line)},
            Ok(_) => {}
        }
        line.clear();
        //if i == 10_000_000 {
        //    break;
        //}
    }
    println!("Converting to recordbatch!");
    let r = occurence_to_recordbatch(&occurence);
    
    let writer_props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    
    println!("Writing .. ");
    let outf = File::create(args.output).expect("failed to create outfile");
    let mut writer = parquet::arrow::ArrowWriter::try_new(outf, r.schema(), Some(writer_props)).unwrap();

    writer.write(&r).expect("Failed to write");

    writer.close().unwrap();
}
