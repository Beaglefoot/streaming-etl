CREATE TABLE room_dim (
    room_key int PRIMARY KEY NOT NULL,
    room_id int NOT NULL,
    title varchar(100) NOT NULL,
    description varchar(1024) NOT NULL,
    foundation_time varchar(26) NOT NULL,
    row_effective_time varchar(26) NOT NULL,
    row_expiration_time varchar(26) DEFAULT '9999-01-01T00:00:00' NOT NULL,
    current_row_indicator varchar(7) NOT NULL
);

CREATE OR REPLACE FUNCTION expire_previous_room_row() RETURNS trigger AS $expire_previous_room_row$
    BEGIN
        UPDATE room_dim
            SET row_expiration_time = NEW.row_effective_time,
                current_row_indicator = 'Expired'
        WHERE room_id = NEW.room_id
            AND current_row_indicator = 'Current';

        RETURN NEW;
    END
$expire_previous_room_row$ LANGUAGE plpgsql;

CREATE TRIGGER expire_previous_room_row BEFORE INSERT ON room_dim
    FOR EACH ROW EXECUTE FUNCTION expire_previous_room_row();

CREATE INDEX IF NOT EXISTS room_dim_room_id_idx ON room_dim (room_id);
