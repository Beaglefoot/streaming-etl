DROP TABLE IF EXISTS "user";

CREATE TABLE "user" (
    user_id serial PRIMARY KEY NOT NULL,
    username varchar(20) UNIQUE,
    first_name varchar(40) NOT NULL,
    last_name varchar(40) NOT NULL,
    email varchar(40) NOT NULL,
    registration_time timestamp DEFAULT now() NOT NULL
);
