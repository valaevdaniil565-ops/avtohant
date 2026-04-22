-- Extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================
-- vacancies
-- =========================
CREATE TABLE IF NOT EXISTS vacancies (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  synthetic_id VARCHAR(64) UNIQUE,

  role              VARCHAR(255) NOT NULL,
  stack             JSONB NOT NULL DEFAULT '[]'::jsonb,
  grade             VARCHAR(50),
  experience_years  INTEGER,
  rate_min          INTEGER,
  rate_max          INTEGER,
  currency          VARCHAR(10) DEFAULT 'RUB',
  company           VARCHAR(255),
  location          VARCHAR(255),
  description       TEXT,
  original_text     TEXT NOT NULL,

  embedding         VECTOR(768),

  status            VARCHAR(20) DEFAULT 'active', -- active, closed, hidden
  is_strategic      BOOLEAN DEFAULT FALSE,

  created_at        TIMESTAMPTZ DEFAULT NOW(),
  updated_at        TIMESTAMPTZ DEFAULT NOW(),
  expires_at        TIMESTAMPTZ,
  closed_at         TIMESTAMPTZ,
  close_reason      VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_vacancies_status   ON vacancies(status);
CREATE INDEX IF NOT EXISTS idx_vacancies_expires  ON vacancies(expires_at);
CREATE INDEX IF NOT EXISTS idx_vacancies_synth    ON vacancies(synthetic_id);

-- Векторный индекс (IVFFLAT): его имеет смысл создавать после наполнения и установки lists
-- но для MVP можно создать сразу и позже пересоздать.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE indexname = 'idx_vacancies_embedding'
  ) THEN
    EXECUTE 'CREATE INDEX idx_vacancies_embedding ON vacancies USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)';
  END IF;
END$$;

-- =========================
-- specialists
-- =========================
CREATE TABLE IF NOT EXISTS specialists (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  synthetic_id VARCHAR(64) UNIQUE,

  role              VARCHAR(255) NOT NULL,
  stack             JSONB NOT NULL DEFAULT '[]'::jsonb,
  grade             VARCHAR(50),
  experience_years  INTEGER,
  rate_min          INTEGER,
  rate_max          INTEGER,
  currency          VARCHAR(10) DEFAULT 'RUB',
  location          VARCHAR(255),
  description       TEXT,
  original_text     TEXT NOT NULL,

  embedding         VECTOR(768),

  status            VARCHAR(20) DEFAULT 'active', -- active, hired, hidden
  is_internal       BOOLEAN DEFAULT FALSE,

  created_at        TIMESTAMPTZ DEFAULT NOW(),
  updated_at        TIMESTAMPTZ DEFAULT NOW(),
  expires_at        TIMESTAMPTZ,
  hired_at          TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_specialists_status  ON specialists(status);
CREATE INDEX IF NOT EXISTS idx_specialists_expires ON specialists(expires_at);
CREATE INDEX IF NOT EXISTS idx_specialists_synth   ON specialists(synthetic_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE indexname = 'idx_specialists_embedding'
  ) THEN
    EXECUTE 'CREATE INDEX idx_specialists_embedding ON specialists USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)';
  END IF;
END$$;

-- =========================
-- channels
-- =========================
CREATE TABLE IF NOT EXISTS channels (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  telegram_id   BIGINT UNIQUE NOT NULL,
  title         VARCHAR(255) NOT NULL,
  username      VARCHAR(255),
  source_kind   VARCHAR(20) DEFAULT 'chat',
  is_active     BOOLEAN DEFAULT TRUE,
  is_strategic  BOOLEAN DEFAULT FALSE,
  ttl_months    INTEGER DEFAULT 1,
  last_message_id BIGINT,
  added_at      TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- =========================
-- sources
-- ВАЖНО: для MVP делаем entity_type/entity_id nullable,
-- чтобы сначала сохранять raw, а потом воркер проставлял связь.
-- =========================
CREATE TABLE IF NOT EXISTS sources (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  entity_type   VARCHAR(20),     -- vacancy, specialist (nullable в момент ingest)
  entity_id     UUID,            -- (nullable в момент ingest)

  channel_id    BIGINT NOT NULL,
  message_id    BIGINT NOT NULL,
  chat_title    VARCHAR(255),
  sender_id     BIGINT,
  sender_name   VARCHAR(255),
  message_url   VARCHAR(512),
  external_url  VARCHAR(1024),
  external_kind VARCHAR(64),
  external_locator VARCHAR(128),

  source_type   VARCHAR(20) DEFAULT 'auto', -- auto, manual, forward
  raw_text      TEXT,
  source_meta   JSONB,

  has_attachment   BOOLEAN DEFAULT FALSE,
  attachment_type  VARCHAR(50),
  attachment_path  VARCHAR(512),

  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Уникальность источника по сообщению
CREATE UNIQUE INDEX IF NOT EXISTS idx_sources_message ON sources(channel_id, message_id);

-- Для быстрых джойнов к сущности
CREATE INDEX IF NOT EXISTS idx_sources_entity ON sources(entity_type, entity_id);

-- =========================
-- tg_archive_map
-- =========================
CREATE TABLE IF NOT EXISTS tg_archive_map (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  original_chat_id   BIGINT NOT NULL,
  original_message_id BIGINT NOT NULL,
  origin_type        VARCHAR(32) NOT NULL, -- channel_original, chat_archived
  classification     VARCHAR(20),          -- bench, vacancy, irrelevant
  archive_chat_id    BIGINT,
  archive_message_id BIGINT,
  canonical_message_url VARCHAR(512),
  archive_post_status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, posted, skipped, failed
  archive_posted_at  TIMESTAMPTZ,
  last_error         TEXT,
  created_at         TIMESTAMPTZ DEFAULT NOW(),
  updated_at         TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(original_chat_id, original_message_id),
  UNIQUE(archive_chat_id, archive_message_id)
);

CREATE INDEX IF NOT EXISTS idx_tg_archive_map_status ON tg_archive_map(archive_post_status);
CREATE INDEX IF NOT EXISTS idx_tg_archive_map_origin_type ON tg_archive_map(origin_type);

-- =========================
-- matches
-- =========================
CREATE TABLE IF NOT EXISTS matches (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vacancy_id       UUID NOT NULL REFERENCES vacancies(id) ON DELETE CASCADE,
  specialist_id    UUID NOT NULL REFERENCES specialists(id) ON DELETE CASCADE,
  similarity_score DOUBLE PRECISION NOT NULL, -- 0..1
  rank             INTEGER NOT NULL,
  is_notified      BOOLEAN DEFAULT FALSE,
  hr_feedback      VARCHAR(20), -- relevant, irrelevant
  created_at       TIMESTAMPTZ DEFAULT NOW(),
  updated_at       TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(vacancy_id, specialist_id)
);

CREATE INDEX IF NOT EXISTS idx_matches_vacancy     ON matches(vacancy_id);
CREATE INDEX IF NOT EXISTS idx_matches_specialist  ON matches(specialist_id);
CREATE INDEX IF NOT EXISTS idx_matches_score       ON matches(similarity_score DESC);

-- =========================
-- entity_aliases (для нормализации компаний/технологий)
-- =========================
CREATE TABLE IF NOT EXISTS entity_aliases (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  canonical_name VARCHAR(255) NOT NULL,
  alias          VARCHAR(255) NOT NULL,
  entity_type    VARCHAR(50) DEFAULT 'company',
  created_at     TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(alias)
);

CREATE INDEX IF NOT EXISTS idx_aliases_lookup ON entity_aliases(LOWER(alias));

-- =========================
-- partner_companies
-- =========================
CREATE TABLE IF NOT EXISTS partner_companies (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  canonical_name VARCHAR(255) NOT NULL UNIQUE,
  mentions_count INTEGER NOT NULL DEFAULT 0,
  source_url     VARCHAR(1024),
  created_at     TIMESTAMPTZ DEFAULT NOW(),
  updated_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_partner_companies_mentions ON partner_companies(mentions_count DESC);

-- =========================
-- app_settings
-- =========================
CREATE TABLE IF NOT EXISTS app_settings (
  key        VARCHAR(128) PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =========================
-- audit_log
-- =========================
CREATE TABLE IF NOT EXISTS audit_log (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type  VARCHAR(128) NOT NULL,
  actor       VARCHAR(128) NOT NULL DEFAULT 'system',
  entity_type VARCHAR(64),
  entity_id   VARCHAR(128),
  payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_event_type ON audit_log(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log(entity_type, entity_id, created_at DESC);

-- =========================
-- own_specialists_registry
-- =========================
CREATE TABLE IF NOT EXISTS own_specialists_registry (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  registry_key   VARCHAR(128) NOT NULL UNIQUE,
  source_url     VARCHAR(1024) NOT NULL,
  locator        VARCHAR(255),
  raw_text       TEXT,
  parsed_payload JSONB,
  specialist_id  UUID REFERENCES specialists(id) ON DELETE SET NULL,
  parse_status   VARCHAR(20) NOT NULL DEFAULT 'pending',
  is_active      BOOLEAN NOT NULL DEFAULT TRUE,
  last_error     TEXT,
  last_synced_at TIMESTAMPTZ,
  created_at     TIMESTAMPTZ DEFAULT NOW(),
  updated_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_own_specialists_registry_source ON own_specialists_registry(source_url);

-- =========================
-- jobs
-- =========================
CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  kind VARCHAR(64) NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'queued',
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  result JSONB NOT NULL DEFAULT '{}'::jsonb,
  error TEXT,
  dedupe_key VARCHAR(255),
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  locked_by VARCHAR(128),
  locked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_available ON jobs(status, available_at);
CREATE INDEX IF NOT EXISTS idx_jobs_kind_created ON jobs(kind, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_dedupe_key ON jobs(dedupe_key) WHERE dedupe_key IS NOT NULL;
