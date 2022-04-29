CREATE TABLE user_dim (
    user_key int PRIMARY KEY NOT NULL,
    user_id int NOT NULL,
    username varchar(20) NOT NULL,
    first_name varchar(40) NOT NULL,
    last_name varchar(40) NOT NULL,
    email varchar(40) NOT NULL,
    registration_time varchar(19) NOT NULL,
    row_effective_time varchar(19) NOT NULL,
    row_expiration_time varchar(19) DEFAULT '9999-01-01T00:00:00' NOT NULL,
    current_row_indicator varchar(7) NOT NULL
);

CREATE OR REPLACE FUNCTION expire_previous_user_row() RETURNS trigger AS $expire_previous_user_row$
    BEGIN
        UPDATE user_dim
            SET row_expiration_time = NEW.row_effective_time,
                current_row_indicator = 'Expired'
        WHERE user_id = NEW.user_id
            AND current_row_indicator = 'Current';

        RETURN NEW;
    END
$expire_previous_user_row$ LANGUAGE plpgsql;

CREATE TRIGGER expire_previous_user_row BEFORE INSERT ON user_dim
    FOR EACH ROW EXECUTE FUNCTION expire_previous_user_row();

CREATE INDEX IF NOT EXISTS user_dim_user_id_idx ON user_dim (user_id);
