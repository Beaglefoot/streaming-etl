DROP TABLE IF EXISTS room;

CREATE TABLE room (
    room_id serial PRIMARY KEY NOT NULL,
    title varchar(100) NOT NULL,
    description varchar(1024) NOT NULL,
    foundation_time timestamp DEFAULT now() NOT NULL
);