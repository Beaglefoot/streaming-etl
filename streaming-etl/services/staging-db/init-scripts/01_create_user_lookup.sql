CREATE SEQUENCE user_key_seq START 1000;

CREATE TABLE user_lookup (
    user_id int PRIMARY KEY NOT NULL,
    user_key int DEFAULT nextval('user_key_seq')
);
