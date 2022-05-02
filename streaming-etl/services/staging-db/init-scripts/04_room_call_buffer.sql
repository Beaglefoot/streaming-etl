CREATE TABLE room_call_buffer (
    room_id int NOT NULL,
    call_id int NOT NULL
);

CREATE INDEX room_call_buffer_call_id_idx ON room_call_buffer (call_id);
