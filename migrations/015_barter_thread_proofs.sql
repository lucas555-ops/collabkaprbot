-- Proofs for barter threads (simple evidence: link or screenshot)

create table if not exists barter_thread_proofs (
  id bigserial primary key,
  thread_id bigint not null references barter_threads(id) on delete cascade,
  kind text not null check (kind in ('LINK', 'SCREENSHOT')),
  url text,
  tg_file_id text,
  tg_file_unique_id text,
  added_by_user_id bigint references users(id) on delete set null,
  created_at timestamptz not null default now()
);

create index if not exists idx_barter_thread_proofs_thread_id
  on barter_thread_proofs (thread_id, created_at desc);

create index if not exists idx_barter_thread_proofs_added_by
  on barter_thread_proofs (added_by_user_id, created_at desc);
