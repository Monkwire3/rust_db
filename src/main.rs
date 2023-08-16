use std::{fs, path::Path, sync::Arc};

use parquet::{
    data_type::Int32Type,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
    file::reader::{FileReader, SerializedFileReader},
    column::reader::{ColumnReader, get_typed_column_reader},
};

fn write_file() {
    let path = Path::new("./sample.parquet");

    let message_type = "
  message schema {
    REQUIRED INT32 nums;
  }
";
    let schema = Arc::new(parse_message_type(message_type).unwrap());

    let file = fs::File::create(&path).unwrap();

    let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();

    let mut row_group_writer = writer.next_row_group().unwrap();
    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // ... write values to a column writer
        let values: [i32; 5] = [1, 2, 3, 4, 5];

        col_writer
            .typed::<Int32Type>()
            .write_batch(&values, None, None)
            .unwrap();
        col_writer.close().unwrap()
    }
    row_group_writer.close().unwrap();
    writer.close().unwrap();

    let bytes = fs::read(&path).unwrap();
    assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
}

fn read_file() {
    let path = Path::new("./sample.parquet");

    if let Ok(file) = fs::File::open(&path) {
        let reader = SerializedFileReader::new(file).unwrap();

        let parquet_metadata = reader.metadata();
        println!("parquet_metadata: {:?}", parquet_metadata);

        assert_eq!(parquet_metadata.num_row_groups(), 1);

        let row_group_reader = reader.get_row_group(0).unwrap();
        println!("num columns: {}", row_group_reader.num_columns());

        let mut values = [0; 5];

        let column_reader = row_group_reader.get_column_reader(0).unwrap();

        let mut column_reader = get_typed_column_reader::<Int32Type>(column_reader);
        column_reader.read_records(5, None, None, &mut values).unwrap();


        println!("values: {:?}", values);








    }
}

fn main() {
    // write_file();
    read_file();
}
