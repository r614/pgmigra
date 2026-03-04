alter table "public"."accounts2" enable row level security;

drop policy "account_managers" on "public"."accounts";

create policy "account_managers"
on "public"."accounts"
as restrictive
for all
to schemainspect_test_role
using ((manager = CURRENT_USER));
