# DECISIONS.md

1. **Indexes.** Unique `emp_code` on employees and unique `(emp_code, date)` on logs, so duplicates are impossible at the database level. `(date desc, emp_code)` matches the list sort exactly and also serves month ranges and the leaderboard. `(status, date, emp_code)` is for the status filter, and `(emp_code, punch_in)` finds the record to punch out. Two department indexes on employees feed the analytics. I rejected a plain `status` index because it matches too many rows to help.

2. **Punch-in race.** I don't check first. Both requests call `insert_one`, and the unique `(emp_code, date)` index lets only one of them in. The other gets `DuplicateKeyError`, which I return as 409.

3. **Ties.** I rank with `$rank`, so tied people share a rank, and only then keep rows with rank at most `limit`. If three people tie at the cutoff, all three come back, so the list can be longer than `limit`.

4. **Headcount.** The summary starts from `employees`, not from the logs, and `$unwind` keeps people who have no logs. They still count towards the department's headcount.

5. **100x the data.** I would pre-compute monthly totals per employee when records are written, so analytics read small summary documents. Then I would shard the logs by `emp_code`.
