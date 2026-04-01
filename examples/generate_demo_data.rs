use arrow::array::*;
use arrow::datatypes::*;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::format::KeyValue;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

const CLICKHOUSE_CREATED_BY: &str = "ClickHouse 24.3.1.2672";

const FIRST_NAMES: &[&str] = &[
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack", "Karen",
    "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor",
    "Wendy", "Xander", "Yara", "Zane", "Anna", "Ben", "Clara", "David", "Elena", "Felix", "Gina",
    "Hugo", "Iris", "James", "Kate", "Liam", "Maya", "Nora", "Oscar", "Pia", "Raj", "Sara", "Tom",
    "Vera", "Will", "Xena", "Yuki", "Zara",
];

const LAST_NAMES: &[&str] = &[
    "Smith", "Johnson", "Brown", "Taylor", "Anderson", "Wilson", "Moore", "Clark", "Walker",
    "Hall", "Allen", "Young", "King", "Wright", "Hill", "Scott", "Adams", "Baker", "Carter",
    "Evans", "Garcia", "Harris", "Lee", "Martin", "Nelson", "Patel", "Roberts", "Thomas", "Turner",
    "White",
];

const DEPARTMENTS: &[&str] = &[
    "Engineering",
    "Sales",
    "Marketing",
    "Support",
    "Finance",
    "Product",
    "Design",
    "Operations",
];

const PRODUCTS: &[&str] = &[
    "Laptop",
    "Keyboard",
    "Mouse",
    "Monitor",
    "Headphones",
    "Webcam",
    "Dock",
    "Cable",
    "Charger",
    "Tablet",
    "Phone",
    "Speaker",
    "SSD",
    "RAM",
    "GPU",
];

const STATUSES: &[&str] = &["pending", "shipped", "delivered", "cancelled"];

fn main() {
    let output_dir = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());
    let dir = Path::new(&output_dir);
    std::fs::create_dir_all(dir).expect("failed to create output directory");

    let mut rng = StdRng::seed_from_u64(42);

    generate_users(dir, &mut rng);
    generate_orders(dir, &mut rng, "orders.parquet", 300, 2023);
    generate_orders(dir, &mut rng, "orders_2024.parquet", 200, 2024);

    eprintln!("Generated demo data in {}", dir.display());
}

fn clickhouse_kv(table: &str, database: &str) -> Vec<KeyValue> {
    vec![
        KeyValue {
            key: "ch-table".to_string(),
            value: Some(table.to_string()),
        },
        KeyValue {
            key: "ch-database".to_string(),
            value: Some(database.to_string()),
        },
        KeyValue {
            key: "ch-query-id".to_string(),
            value: Some("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()),
        },
        KeyValue {
            key: "ch-server-version".to_string(),
            value: Some("24.3.1.2672".to_string()),
        },
    ]
}

fn generate_users(dir: &Path, rng: &mut StdRng) {
    let path = dir.join("users.parquet");
    let n = 500;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("salary", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("department", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(250)
        .set_created_by(CLICKHOUSE_CREATED_BY.to_string())
        .set_key_value_metadata(Some(clickhouse_kv("users", "analytics")))
        .build();

    let file = File::create(&path).expect("create users.parquet");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let ids: Vec<i64> = (1..=n as i64).collect();
    let names: Vec<String> = (0..n)
        .map(|_| {
            let first = FIRST_NAMES[rng.random_range(0..FIRST_NAMES.len())];
            let last = LAST_NAMES[rng.random_range(0..LAST_NAMES.len())];
            format!("{first} {last}")
        })
        .collect();
    let emails: Vec<String> = names
        .iter()
        .map(|name| {
            let parts: Vec<&str> = name.split_whitespace().collect();
            format!(
                "{}.{}@example.com",
                parts[0].to_lowercase(),
                parts[1].to_lowercase()
            )
        })
        .collect();
    let ages: Vec<i32> = (0..n).map(|_| rng.random_range(22..68)).collect();
    let salaries: Vec<f64> = (0..n)
        .map(|_| {
            let base: f64 = rng.random_range(35_000.0..250_000.0);
            (base * 100.0).round() / 100.0
        })
        .collect();
    let actives: Vec<bool> = (0..n).map(|_| rng.random_range(0..10) > 0).collect();
    let departments: Vec<&str> = (0..n)
        .map(|_| DEPARTMENTS[rng.random_range(0..DEPARTMENTS.len())])
        .collect();

    // Timestamps: 2023-01-01 to 2025-03-01 in millis
    let ts_start: i64 = 1_672_531_200_000; // 2023-01-01T00:00:00Z
    let ts_end: i64 = 1_740_787_200_000; // 2025-03-01T00:00:00Z
    let timestamps: Vec<i64> = (0..n).map(|_| rng.random_range(ts_start..ts_end)).collect();

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(
                names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                emails.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(ages)),
            Arc::new(Float64Array::from(salaries)),
            Arc::new(BooleanArray::from(actives)),
            Arc::new(StringArray::from(departments)),
            Arc::new(TimestampMillisecondArray::from(timestamps)),
        ],
    )
    .unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
    eprintln!("  {} ({n} rows, 2 row groups, zstd)", path.display());
}

fn generate_orders(dir: &Path, rng: &mut StdRng, filename: &str, n: usize, year: i32) {
    let path = dir.join(filename);

    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("product", DataType::Utf8, true),
        Field::new("quantity", DataType::Int32, true),
        Field::new("price", DataType::Float64, true),
        Field::new("status", DataType::Utf8, true),
    ]));

    let table_name = filename.trim_end_matches(".parquet");
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(CLICKHOUSE_CREATED_BY.to_string())
        .set_key_value_metadata(Some(clickhouse_kv(table_name, "analytics")))
        .build();

    let file = File::create(&path).expect("create orders parquet");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let base_id: i64 = if year == 2024 { 10_000 } else { 1 };
    let order_ids: Vec<i64> = (base_id..base_id + n as i64).collect();
    let user_ids: Vec<i64> = (0..n).map(|_| rng.random_range(1..=500)).collect();
    let products: Vec<&str> = (0..n)
        .map(|_| PRODUCTS[rng.random_range(0..PRODUCTS.len())])
        .collect();
    let quantities: Vec<i32> = (0..n).map(|_| rng.random_range(1..20)).collect();
    let prices: Vec<f64> = (0..n)
        .map(|_| {
            let p: f64 = rng.random_range(9.99..2_499.99);
            (p * 100.0).round() / 100.0
        })
        .collect();
    let statuses: Vec<&str> = (0..n)
        .map(|_| STATUSES[rng.random_range(0..STATUSES.len())])
        .collect();

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(order_ids)),
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(StringArray::from(products)),
            Arc::new(Int32Array::from(quantities)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(StringArray::from(statuses)),
        ],
    )
    .unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
    eprintln!("  {} ({n} rows, 1 row group, snappy)", path.display());
}
