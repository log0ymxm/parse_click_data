#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate nom;

extern crate flate2;
extern crate glob;
extern crate serde;
extern crate serde_json;
extern crate time;

use flate2::read::GzDecoder;
use glob::glob;
use nom::digit;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::str::FromStr;
use std::str::from_utf8;
use time::PreciseTime;

#[derive(Serialize, Debug)]
struct Visit {
    day: String,
    timestamp: u32,
    displayed_article: String,
    user_clicked: u8,
    user: Vec<f64>,
    articles: HashMap<String, Vec<f64>>,
}

named!(float<&str>, map_res!(recognize!(tuple!(digit, tag!("."), digit)), from_utf8));
named!(float64<f64>, map_res!(float, FromStr::from_str));

named!(digits<&str>, map_res!(digit, from_utf8));
named!(int32<u32>, map_res!(digits, FromStr::from_str));
named!(int8<u8>, map_res!(digits, FromStr::from_str));

named!(indexed_value<&[u8], (u32, f64)>, separated_pair!(int32, tag!(":"), float64));

named!(article<&[u8], (String, Vec<f64>) >,
       do_parse!(
           tag!("|") >>
               article_id: digits >>
               tag!(" ") >>
               article_context: separated_list!(tag!(" "), indexed_value) >>
               ({
                   let mut context = vec![0.0; 6];
                   for (idx, v) in article_context {
                       if idx <= 6 {
                           context[(idx-1) as usize] = v;
                       }
                   }
                   (
                       article_id.to_owned(),
                       context
                   )
               })));

named!(visit_parser<&[u8], (u32, String, u8, Vec<f64>, Vec<(String, Vec<f64>)>)>, do_parse!(
    timestamp: int32 >>
        tag!(" ") >>
        displayed_article: digits >>
        tag!(" ") >>
        user_clicked: int8 >>
        tag!(" |user ") >>
        user_context: separated_list!(tag!(" "), indexed_value) >>
        tag!(" ") >>
        articles: separated_list!(tag!(" "), complete!(article)) >>
        ({
            let mut context = vec![0.0; 6];
            for (idx, v) in user_context {
                context[(idx-1) as usize] = v;
            }
            (
                timestamp,
                displayed_article.to_owned(),
                user_clicked,
                context,
                articles
            )
        }
        )));

fn parse_visit(day: String, line: &[u8]) -> Visit {
    let (timestamp, displayed_article, user_clicked, user_context, articles_list) = visit_parser(line).unwrap().1;
    let mut articles: HashMap<String, Vec<f64>> = HashMap::new();
    for (k, v) in articles_list {
        articles.insert(k, v);
    }
    Visit {
        day: day,
        timestamp: timestamp,
        displayed_article: displayed_article,
        user_clicked: user_clicked,
        user: user_context,
        articles: articles
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        println!("Usage: parse_click_data <data_input_glob> <output_file>\n\nExample: parse_click_data ./webscope_user_click_log/R6/*.gz ./parsed.jsonl");
        std::process::exit(0);
    }

    let inputGlob = &args[1];
    let outputFile = &args[2];

    let mut output = OpenOptions::new()
        .create(true)
        .write(true)
        .open(outputFile)
        .unwrap();

    let start = PreciseTime::now();
    let mut i = 0;

    for entry in glob(inputGlob).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => {
                println!("File: {}", path.display());
                let file = File::open(&path).unwrap();
                let d = GzDecoder::new(file).unwrap();
                let reader = BufReader::new(d);

                for line in reader.lines() {
                    let l = line.unwrap();
                    let day = path.file_name().unwrap().to_string_lossy().split(".").nth(1).unwrap().to_owned();
                    //println!("- {:?}", l);
                    let visit = parse_visit(day, l.as_bytes());
                    //println!("- visit {:?}", visit);
                    let json = serde_json::to_string(&visit).unwrap();
                    //println!("- visit {:?}", json);
                    let _ = writeln!(output, "{}", json);

                    if i % 1000 == 0 {
                        let now = PreciseTime::now();
                        let secs = start.to(now).num_seconds() + 1;
                        let its_per_sec = i / secs;
                        print!("\r{} ({} it/s)", i, its_per_sec);
                        std::io::stdout().flush().unwrap();
                    }
                    i += 1;
                }
                print!("\n");
            }
            Err(e) => println!("Error: {:?}", e)
        }
    }

    let end = PreciseTime::now();
    println!("Done {}", start.to(end).num_seconds());
}
