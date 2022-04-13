DROP TABLE IF EXISTS "call";

CREATE TABLE "call" (
    call_id serial PRIMARY KEY NOT NULL,
    start_time timestamp DEFAULT now() NOT NULL,
    end_time timestamp
);