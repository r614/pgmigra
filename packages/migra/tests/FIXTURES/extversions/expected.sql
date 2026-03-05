alter extension "pg_trgm" update to '1.4';

create extension if not exists "citext" with schema "public" version '1.5';

drop extension if exists "hstore";
