-- Step 4 (pgvector) table alignment for Supabase.
-- Run this in Supabase SQL Editor (NOT via transaction pooler).
--
-- Notes:
-- - Enable the "vector" extension in Supabase (Database → Extensions) if CREATE EXTENSION is blocked.
-- - This script is idempotent for missing columns. It does not drop/rename existing columns.

-- 1) pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- 2) Ensure the target table exists (name used by Step 4 config)
CREATE TABLE IF NOT EXISTS public.topics_embeddings (
  topic_id uuid PRIMARY KEY,
  workspace_id text NOT NULL,
  batch_id text,
  embedding_provider text,
  embedding_model text,
  embedding vector(768) NOT NULL,
  topic_metadata jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- 3) If the table already exists, ensure required columns exist.
ALTER TABLE public.topics_embeddings
  ADD COLUMN IF NOT EXISTS topic_id uuid;

ALTER TABLE public.topics_embeddings
  ADD COLUMN IF NOT EXISTS workspace_id text;

ALTER TABLE public.topics_embeddings
  ADD COLUMN IF NOT EXISTS batch_id text;

ALTER TABLE public.topics_embeddings
  ADD COLUMN IF NOT EXISTS embedding_provider text;

ALTER TABLE public.topics_embeddings
  ADD COLUMN IF NOT EXISTS embedding_model text;

ALTER TABLE public.topics_embeddings
  ADD COLUMN IF NOT EXISTS embedding vector(768);

ALTER TABLE public.topics_embeddings
  ADD COLUMN IF NOT EXISTS topic_metadata jsonb;

ALTER TABLE public.topics_embeddings
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE public.topics_embeddings
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

-- 4) Constraints (best-effort)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'topics_embeddings_pkey'
  ) THEN
    BEGIN
      ALTER TABLE public.topics_embeddings ADD PRIMARY KEY (topic_id);
    EXCEPTION WHEN others THEN
      -- ignore if table already has a PK or existing data violates it
      NULL;
    END;
  END IF;
END $$;

-- 5) Helpful indexes for Step 6 search patterns
CREATE INDEX IF NOT EXISTS topics_embeddings_workspace_id_idx
  ON public.topics_embeddings (workspace_id);

-- Optional: vector index (recommended when data grows; requires ANALYZE for best results).
-- CREATE INDEX IF NOT EXISTS topics_embeddings_embedding_ivfflat
--   ON public.topics_embeddings USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
-- ANALYZE public.topics_embeddings;

