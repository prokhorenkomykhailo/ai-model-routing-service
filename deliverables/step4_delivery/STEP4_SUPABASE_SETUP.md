# Step 4 (pgvector) — Supabase setup

## 1) Enable pgvector
In Supabase Dashboard:
- Database → Extensions → enable `vector`

## 2) Align the table schema
Run in Supabase SQL Editor:
- `deliverables/step4_delivery/SUPABASE_TOPICS_EMBEDDINGS_MIGRATION.sql`

## 3) Configure the service (local run)
Set env vars (keep secrets out of git):
- `GEMINI_API_KEY` (optional; Step 4 falls back deterministically if unset)
- `TOPIC_EMBEDDING_PGVECTOR_JDBC_URL` (Supabase connection string with SSL)
- `TOPIC_EMBEDDING_PGVECTOR_USERNAME`
- `TOPIC_EMBEDDING_PGVECTOR_PASSWORD`
- `TOPIC_EMBEDDING_PGVECTOR_SCHEMA=public`
- `TOPIC_EMBEDDING_PGVECTOR_TABLE=topics_embeddings`
- `TOPIC_EMBEDDING_PGVECTOR_AUTO_DDL=false`

## 4) Verify writes
In Supabase SQL Editor:
```sql
select count(*) from public.topics_embeddings;
select topic_id, workspace_id, updated_at from public.topics_embeddings order by updated_at desc limit 10;
```

