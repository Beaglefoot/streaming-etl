DROP TABLE IF EXISTS room_call;

CREATE TABLE room_call (
    room_id int REFERENCES "room" (room_id) NOT NULL,
    call_id int REFERENCES "call" (call_id) NOT NULL
);