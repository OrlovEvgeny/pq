# pq demo - Parquet CLI toolkit
#
# Before recording:
#   cargo build --release
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
Type "if [ ! -x target/release/examples/generate_demo_data ]; then cargo build --release --example generate_demo_data >/dev/null 2>&1; fi"
Enter
Sleep 500ms
Type "rm -rf /tmp/pq-demo && target/release/examples/generate_demo_data /tmp/pq-demo >/dev/null 2>&1 && cd /tmp/pq-demo"
Enter
Sleep 500ms
Type "clear"
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

# 3. SQL aggregation
Enter
Type "pq query 'SELECT department, ROUND(AVG(salary), 2) AS avg_salary, COUNT(*) AS employees FROM users GROUP BY department ORDER BY avg_salary DESC LIMIT 5' users.parquet"
Sleep 500ms
Enter
Sleep 3s

# 4. Count
Enter
Type "pq count *.parquet"
Sleep 500ms
Enter
Sleep 2.5s

# 5. DDL
Enter
Type "pq schema extract --ddl duckdb users.parquet"
Sleep 500ms
Enter
Sleep 3s

# 6. Size
Enter
Type "pq size users.parquet"
Sleep 500ms
Enter
Sleep 3s

Sleep 1s
