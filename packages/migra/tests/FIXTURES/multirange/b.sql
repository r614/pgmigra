CREATE TYPE floatrange AS RANGE (subtype = float8);

CREATE TABLE measurements (
    id serial primary key,
    ranges floatmultirange
);
