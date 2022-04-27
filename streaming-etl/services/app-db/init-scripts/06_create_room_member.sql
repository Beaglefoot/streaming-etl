DROP TABLE IF EXISTS room_member;

CREATE TABLE room_member (
    room_id int REFERENCES "room" (room_id) NOT NULL,
    user_id int REFERENCES "user" (user_id) NOT NULL
);
