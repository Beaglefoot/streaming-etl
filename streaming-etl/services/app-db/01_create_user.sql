DROP TABLE IF EXISTS "user";

CREATE TABLE "user" (
    user_id serial PRIMARY KEY NOT NULL,
    first_name varchar(40) NOT NULL,
    last_name varchar(40) NOT NULL,
    email varchar(40) NOT NULL
);