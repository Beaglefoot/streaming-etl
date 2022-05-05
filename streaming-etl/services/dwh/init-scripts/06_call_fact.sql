CREATE TABLE call_fact (
    participant_group_key int NOT NULL,
    room_key int NOT NULL,
    start_date_key int REFERENCES date_dim (date_key) NOT NULL,
    end_date_key int REFERENCES date_dim (date_key) NOT NULL,
    start_time varchar(26) NOT NULL,
    end_time varchar(26) NOT NULL
);
