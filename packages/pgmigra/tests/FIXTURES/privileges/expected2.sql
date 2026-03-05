revoke delete on table "public"."any_table" from "schemainspect_test_role";

drop view if exists "public"."any_other_view";

grant update on table "public"."any_table" to "schemainspect_test_role";
