# DECISIONS.md

1. **Indexes.** Unique `emp_code` on employees and unique `(emp_code, date)` on logs guarantee no duplicates. `(date desc, emp_code)` serves the list sort, month ranges and the leaderboard. `(status, date, emp_code)` serves the status filter. `(emp_code, punch_in)` finds the record to punch out. Employees also get `(department, emp_code)` and `(department, joined_on)`. I rejected a single-field `status` index: too unselective to help.
2. **Punch-in race.** Both requests skip any read-then-write and just call `insert_one`. The unique `(emp_code, date)` index lets MongoDB accept exactly one. The other gets `DuplicateKeyError`, which I turn into 409.
3. **Ties.** The leaderboard ranks with `$rank`, so tied employees share a rank and `limit` is applied afterwards on `rank <= limit`. A tie at the cutoff returns every tied employee, so there can be more rows than `limit`.
4. **Headcount.** The summary starts from `employees` (joined on or before month end), not from the logs. `$lookup` brings each person's logs and `$unwind` keeps people with none. Headcount is the number of distinct employees in each department group.
5. **100x data.** Pre-aggregate monthly numbers per employee into a summary collection when records are written, so analytics read thousands of small documents. Then shard logs by `emp_code`.
