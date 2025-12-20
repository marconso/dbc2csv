use std::env;
use std::collections::HashMap;
use std::fs::File;
use std::fs::remove_file;
use std::io::Write;
use ftp::FtpStream;
use dbase::FieldValue;


fn fetch_data(path_to_save: &str, path_file_dbc: &str) {
    let mut ftp_stream = FtpStream::connect("ftp.datasus.gov.br:21")
        .unwrap_or_else(|err| panic!("{}", err));
    ftp_stream.login("anonymous", "anonymous").unwrap();
    let file_dbc = path_file_dbc.split("/").last().unwrap();
    ftp_stream.retr(
        path_file_dbc,
        |stream| {
            println!("Baixando arquivo: {}", file_dbc);
            let mut writer = File::create(format!("{}/{}", path_to_save, file_dbc)).unwrap();
            let mut data = Vec::new();
            stream.read_to_end(&mut data).unwrap();
            writer.write_all(&data).unwrap();
            Ok(())
    }).unwrap();
    ftp_stream.quit().unwrap();
    println!("Arquivo {} baixado com sucesso", file_dbc);
}


fn dbc2csv(input: &str) {
    let output = input.replace(".dbc", ".dbf").replace(".DBC", ".dbf");
    let output_csv = output.replace(".dbf", ".csv");
    datasus_dbc::decompress(input, &output).unwrap();

    println!("Convertendo: {} -> {}", input, output_csv);

    let mut records = dbase::Reader::from_path(&output).unwrap();
    let fields_names_vec: Vec<&str> = records.fields().iter().map(|f| f.name()).collect();

    let mut file = std::fs::File::create(output_csv).unwrap();

    let mut rows: HashMap<String, String> = HashMap::new();
    for field in fields_names_vec {
        rows.insert(field.to_string(), "".to_string());
    }

    for (e, record) in records.iter_records().enumerate() {
        let record = record.unwrap();
        for (col, val) in record {
            if let FieldValue::Character(Some(string)) = val {
                *rows.get_mut(&col).unwrap() = string;
            }
        }
        if e == 0 {
            file.write_all(rows.keys().cloned().collect::<Vec<String>>().join(",").as_bytes()).unwrap();
            file.write_all(b"\n").unwrap();
            file.write_all(rows.values().cloned().collect::<Vec<String>>().join(",").as_bytes()).unwrap();
            file.write_all(b"\n").unwrap();
        } else {
            file.write_all(rows.values().cloned().collect::<Vec<String>>().join(",").as_bytes()).unwrap();
            file.write_all(b"\n").unwrap();
        }
    }
    remove_file(input).unwrap();
    remove_file(output).unwrap();
}


fn main() {
    let args: Vec<String> = env::args().collect();

    let path_downloads = &args[1];
    let file_dbc = &args[2];
    let input = file_dbc.split("/").last().unwrap();

    fetch_data(path_downloads, file_dbc);
    dbc2csv(&format!("{}/{}", path_downloads, input));
}
