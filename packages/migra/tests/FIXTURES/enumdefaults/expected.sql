alter type "public"."order_status" add value 'rejected' after 'complete';

alter table "public"."orders" alter column "othercolumn" set data type other.otherenum2 using "othercolumn"::text::other.otherenum2;
