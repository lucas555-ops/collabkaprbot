-- Payments ledger extras: status constraint + helper indexes

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'payments_status_check'
  ) then
    alter table payments
      add constraint payments_status_check
      check (status in ('RECEIVED','APPLIED','ORPHANED','ERROR'));
  end if;
end $$;

-- Admin queue performance helpers
create index if not exists idx_payments_created_at
  on payments (created_at desc);

create index if not exists idx_payments_orphaned_time
  on payments (created_at desc)
  where status = 'ORPHANED';
