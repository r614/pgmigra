create type "public"."floatrange" as range (
    subtype = double precision
);

create sequence "public"."measurements_id_seq";

create table "public"."measurements" (
    "id" integer not null default nextval('measurements_id_seq'::regclass),
    "ranges" floatmultirange
);


alter sequence "public"."measurements_id_seq" owned by "public"."measurements"."id";

CREATE UNIQUE INDEX measurements_pkey ON public.measurements USING btree (id);

alter table "public"."measurements" add constraint "measurements_pkey" PRIMARY KEY using index "measurements_pkey";