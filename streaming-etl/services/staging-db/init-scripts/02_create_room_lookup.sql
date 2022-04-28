CREATE SEQUENCE room_key_seq START 1000;

CREATE TABLE room_lookup (
    room_id int PRIMARY KEY NOT NULL,
    room_key int DEFAULT nextval('room_key_seq')
);
