CREATE TABLE IF NOT EXISTS batch_tracker_commited (
    batch_id INT primary key not null,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);