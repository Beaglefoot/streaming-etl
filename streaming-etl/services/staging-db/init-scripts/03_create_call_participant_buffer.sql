CREATE TABLE call_participant_buffer (
    call_id int NOT NULL,
    user_id int NOT NULL
);

CREATE INDEX call_participant_buffer_call_id_idx ON call_participant_buffer (call_id);
