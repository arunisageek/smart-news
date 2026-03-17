CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS articles (
    id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    url TEXT NOT NULL,
    publication_date TIMESTAMPTZ NOT NULL,
    source_name TEXT NOT NULL,
    category TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    relevance_score DOUBLE PRECISION NOT NULL
        CHECK (relevance_score >= 0 AND relevance_score <= 1),
    llm_summary TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    location GEOGRAPHY(Point, 4326),
    search_document TSVECTOR,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION set_article_derived_fields()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();

    NEW.search_document :=
        setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.description, '')), 'B');

    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.location :=
            ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)::geography;
    ELSE
        NEW.location := NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_articles_derived_fields ON articles;

CREATE TRIGGER trg_articles_derived_fields
BEFORE INSERT OR UPDATE
ON articles
FOR EACH ROW
EXECUTE FUNCTION set_article_derived_fields();

CREATE INDEX IF NOT EXISTS idx_articles_publication_date
    ON articles (publication_date DESC);

CREATE INDEX IF NOT EXISTS idx_articles_source_pubdate
    ON articles (source_name, publication_date DESC);

CREATE INDEX IF NOT EXISTS idx_articles_relevance_score
    ON articles (relevance_score DESC);

CREATE INDEX IF NOT EXISTS idx_articles_category
    ON articles USING GIN (category);

CREATE INDEX IF NOT EXISTS idx_articles_search_document
    ON articles USING GIN (search_document);

CREATE INDEX IF NOT EXISTS idx_articles_location
    ON articles USING GIST (location);