-- CRM stage in Inbox threads (for brands)

alter table barter_threads
  add column if not exists buyer_stage text;

create index if not exists idx_barter_threads_buyer_stage on barter_threads (buyer_user_id, buyer_stage);
