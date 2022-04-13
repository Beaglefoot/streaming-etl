DROP TABLE IF EXISTS call_participant;

CREATE TABLE call_participant (
    call_id int REFERENCES "call" (call_id) NOT NULL,
    user_id int REFERENCES "user" (user_id) NOT NULL,
    join_time timestamp DEFAULT now() NOT NULL,
    leave_time timestamp
);