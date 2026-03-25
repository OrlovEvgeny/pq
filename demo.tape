# pq demo - Parquet CLI toolkit
#
# Before recording:
#   cargo build --release
#   cargo run --release --example generate_demo_data -- /tmp/pq-demo
#
# Record:
#   PATH="$PWD/target/release:$PATH" vhs demo.tape

Output doc/demo.gif

Set Shell "bash"
Set FontSize 18
Set Width 1400
Set Height 800
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful
Set Padding 20
Set TypingSpeed 50ms
Set CursorBlink false

Hide
Type "cd /tmp/pq-demo && clear"
Enter
Sleep 1s
Show

# 1. Inspect
Type "pq users.parquet"
Sleep 500ms
Enter
Sleep 4s

# 2. Head (select columns to fit nicely)
Enter
Type "pq head -n 5 -c id,name,email,salary,department users.parquet"
Sleep 500ms
Enter
Sleep 3s

# 3. Count
Enter
Type "pq count *.parquet"
Sleep 500ms
Enter
Sleep 2.5s

# 4. DDL
Enter
Type "pq schema extract --ddl duckdb users.parquet"
Sleep 500ms
Enter
Sleep 3s

# 5. Size
Enter
Type "pq size users.parquet"
Sleep 500ms
Enter
Sleep 3s

Sleep 1s
