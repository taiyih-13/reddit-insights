-- Reddit Insights Database Schema for Supabase
-- Clean version with proper PostgreSQL syntax

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Domains (Finance, Entertainment, Travel)
CREATE TABLE domains (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert default domains
INSERT INTO domains (name, description) VALUES 
('finance', 'Financial discussions, trading, investing'),
('entertainment', 'Movies, TV shows, gaming, media'),
('travel', 'Travel tips, destinations, regional advice');

-- Categories (subcategories within each domain)
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    domain_id INTEGER REFERENCES domains(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    confidence_threshold DECIMAL(3,2) DEFAULT 0.5,
    created_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(domain_id, name)
);

-- Insert default categories
INSERT INTO categories (domain_id, name, description) VALUES 
-- Finance categories
(1, 'Personal Trading Stories', 'Individual trading experiences and gains/losses'),
(1, 'Analysis & Education', 'Market analysis, educational content, research'),
(1, 'Market News & Politics', 'Financial news, political impacts on markets'),
(1, 'Questions & Help', 'Investment questions and advice requests'),
(1, 'Memes & Entertainment', 'Financial memes and light entertainment'),
(1, 'Community Discussion', 'General financial community discussions'),

-- Entertainment categories  
(2, 'Recommendation Requests', 'Requests for movie/show/game recommendations'),
(2, 'Reviews & Discussions', 'Reviews and discussions of content'),
(2, 'News & Announcements', 'Entertainment industry news and announcements'),
(2, 'Lists & Rankings', 'Top lists and ranking discussions'),
(2, 'Identification & Help', 'Help identifying movies, shows, or games'),

-- Travel categories
(3, 'General Travel Advice', 'General travel tips and planning advice'),
(3, 'Solo Travel', 'Solo travel experiences and advice'),
(3, 'Budget Travel', 'Budget-conscious travel tips and strategies'),
(3, 'Europe', 'European travel destinations and advice'),
(3, 'Asia', 'Asian travel destinations and advice'), 
(3, 'Americas', 'North and South American travel'),
(3, 'Oceania & Africa', 'Oceania and African travel destinations');

-- Subreddits (source communities)
CREATE TABLE subreddits (
    name VARCHAR(50) PRIMARY KEY,
    domain_id INTEGER REFERENCES domains(id) ON DELETE CASCADE,
    display_name VARCHAR(100),
    description TEXT,
    subscriber_count INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    comment_multiplier DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert default subreddits
INSERT INTO subreddits (name, domain_id, display_name, comment_multiplier) VALUES
-- Finance subreddits
('investing', 1, 'r/investing', 1.5),
('stocks', 1, 'r/stocks', 1.5),
('SecurityAnalysis', 1, 'r/SecurityAnalysis', 2.0),
('ValueInvesting', 1, 'r/ValueInvesting', 2.0),
('wallstreetbets', 1, 'r/wallstreetbets', 1.0),
('personalfinance', 1, 'r/personalfinance', 1.2),
('financialindependence', 1, 'r/financialindependence', 1.3),
('pennystocks', 1, 'r/pennystocks', 1.0),
('StockMarket', 1, 'r/StockMarket', 1.2),
('options', 1, 'r/options', 1.3),
('thetagang', 1, 'r/thetagang', 1.4),
('Bitcoin', 1, 'r/Bitcoin', 1.1),
('cryptocurrency', 1, 'r/cryptocurrency', 1.1),
('ethtrader', 1, 'r/ethtrader', 1.1),
('CryptoCurrency', 1, 'r/CryptoCurrency', 1.1),
('daytrading', 1, 'r/daytrading', 1.2),
('forex', 1, 'r/forex', 1.2),

-- Travel subreddits (sample)
('travel', 3, 'r/travel', 1.0),
('solotravel', 3, 'r/solotravel', 1.2),
('backpacking', 3, 'r/backpacking', 1.1),
('JapanTravel', 3, 'r/JapanTravel', 1.0),
('ItalyTravel', 3, 'r/ItalyTravel', 1.0),
('travel_Europe', 3, 'r/travel_Europe', 1.0);

-- Posts (main content table)
CREATE TABLE posts (
    id VARCHAR(20) PRIMARY KEY,
    subreddit VARCHAR(50) REFERENCES subreddits(name) ON DELETE CASCADE,
    title TEXT NOT NULL,
    author VARCHAR(50),
    score INTEGER DEFAULT 0,
    upvote_ratio DECIMAL(4,3),
    num_comments INTEGER DEFAULT 0,
    created_utc TIMESTAMP NOT NULL,
    url TEXT,
    selftext TEXT,
    link_flair_text VARCHAR(100),
    category_id INTEGER REFERENCES categories(id),
    classification_confidence VARCHAR(20),
    popularity_score DECIMAL(10,2),
    engagement_ratio DECIMAL(8,6),
    time_bonus DECIMAL(4,2),
    time_filter VARCHAR(10) NOT NULL,
    extracted_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Sentiment Analysis table
CREATE TABLE sentiment_analysis (
    id SERIAL PRIMARY KEY,
    post_id VARCHAR(20) REFERENCES posts(id) ON DELETE CASCADE,
    sentiment_type VARCHAR(50) NOT NULL,
    sentiment_score DECIMAL(6,4),
    sentiment_label VARCHAR(20),
    positive_score DECIMAL(4,3),
    neutral_score DECIMAL(4,3), 
    negative_score DECIMAL(4,3),
    metadata JSONB,
    computed_at TIMESTAMP DEFAULT NOW()
);

-- Refresh/Update Tracking
CREATE TABLE refresh_logs (
    id SERIAL PRIMARY KEY,
    time_filter VARCHAR(10) NOT NULL,
    domain_name VARCHAR(50),
    posts_added INTEGER DEFAULT 0,
    posts_updated INTEGER DEFAULT 0,
    posts_removed INTEGER DEFAULT 0,
    total_posts_after INTEGER DEFAULT 0,
    duration_seconds DECIMAL(8,2),
    memory_usage_mb INTEGER,
    success BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

-- Summary Cache
CREATE TABLE summary_cache (
    id SERIAL PRIMARY KEY,
    cache_key VARCHAR(200) UNIQUE NOT NULL,
    summary_text TEXT NOT NULL,
    total_posts INTEGER,
    category_breakdown JSONB,
    generated_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    hit_count INTEGER DEFAULT 0
);

-- Dashboard Cache
CREATE TABLE dashboard_cache (
    id SERIAL PRIMARY KEY,
    time_filter VARCHAR(10) NOT NULL,
    domain_name VARCHAR(50) NOT NULL,
    total_posts INTEGER,
    total_upvotes BIGINT,
    avg_score DECIMAL(8,2),
    top_categories JSONB,
    top_subreddits JSONB,
    sentiment_summary JSONB,
    cached_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    
    UNIQUE(time_filter, domain_name)
);

-- Row Level Security (RLS) policies for Supabase
ALTER TABLE posts ENABLE ROW LEVEL SECURITY;
ALTER TABLE sentiment_analysis ENABLE ROW LEVEL SECURITY;
ALTER TABLE refresh_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE summary_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE dashboard_cache ENABLE ROW LEVEL SECURITY;

-- Public read access for dashboard data
CREATE POLICY "Public read access" ON posts FOR SELECT USING (true);
CREATE POLICY "Public read access" ON sentiment_analysis FOR SELECT USING (true);
CREATE POLICY "Public read access" ON categories FOR SELECT USING (true);
CREATE POLICY "Public read access" ON domains FOR SELECT USING (true);
CREATE POLICY "Public read access" ON subreddits FOR SELECT USING (true);
CREATE POLICY "Public read access" ON summary_cache FOR SELECT USING (true);
CREATE POLICY "Public read access" ON dashboard_cache FOR SELECT USING (true);

-- Restrict write access to service role only
CREATE POLICY "Service role write access" ON posts FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role write access" ON sentiment_analysis FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role write access" ON refresh_logs FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role write access" ON summary_cache FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role write access" ON dashboard_cache FOR ALL USING (auth.role() = 'service_role');

-- Useful views for common queries
CREATE VIEW posts_with_details AS
SELECT 
    p.*,
    d.name as domain_name,
    c.name as category_name,
    s.display_name as subreddit_display_name,
    s.comment_multiplier
FROM posts p
LEFT JOIN categories c ON p.category_id = c.id
LEFT JOIN domains d ON c.domain_id = d.id  
LEFT JOIN subreddits s ON p.subreddit = s.name;

CREATE VIEW daily_stats AS
SELECT 
    DATE(created_utc) as date,
    d.name as domain,
    COUNT(*) as total_posts,
    AVG(score) as avg_score,
    AVG(popularity_score) as avg_popularity,
    SUM(score) as total_upvotes
FROM posts p
JOIN categories c ON p.category_id = c.id
JOIN domains d ON c.domain_id = d.id
WHERE created_utc >= NOW() - INTERVAL '30 days'
GROUP BY DATE(created_utc), d.name
ORDER BY date DESC, domain;

-- Functions for common operations
CREATE OR REPLACE FUNCTION cleanup_old_posts(retention_days INTEGER DEFAULT 7)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM posts 
    WHERE created_utc < NOW() - INTERVAL '1 day' * retention_days;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_domain_stats(domain_name TEXT, time_filter_param TEXT DEFAULT 'week')
RETURNS TABLE(
    total_posts BIGINT,
    avg_score NUMERIC,
    avg_popularity NUMERIC,
    top_category TEXT,
    top_subreddit TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(p.id)::BIGINT as total_posts,
        AVG(p.score)::NUMERIC as avg_score,
        AVG(p.popularity_score)::NUMERIC as avg_popularity,
        (SELECT c.name FROM categories c 
         JOIN posts p2 ON p2.category_id = c.id 
         WHERE c.domain_id = d.id 
         GROUP BY c.name 
         ORDER BY COUNT(*) DESC 
         LIMIT 1) as top_category,
        (SELECT p3.subreddit FROM posts p3 
         JOIN categories c2 ON p3.category_id = c2.id 
         WHERE c2.domain_id = d.id 
         GROUP BY p3.subreddit 
         ORDER BY COUNT(*) DESC 
         LIMIT 1) as top_subreddit
    FROM posts p
    JOIN categories c ON p.category_id = c.id
    JOIN domains d ON c.domain_id = d.id
    WHERE d.name = domain_name 
    AND p.time_filter = time_filter_param
    AND p.created_utc >= NOW() - CASE 
        WHEN time_filter_param = 'day' THEN INTERVAL '1 day'
        ELSE INTERVAL '7 days'
    END
    GROUP BY d.id;
END;
$$ LANGUAGE plpgsql;

-- Create Performance Indexes (at the end)
CREATE INDEX idx_posts_subreddit ON posts(subreddit);
CREATE INDEX idx_posts_created_utc ON posts(created_utc);
CREATE INDEX idx_posts_category ON posts(category_id);
CREATE INDEX idx_posts_time_filter ON posts(time_filter);
CREATE INDEX idx_posts_popularity ON posts(popularity_score DESC);
CREATE INDEX idx_posts_score ON posts(score DESC);
CREATE INDEX idx_posts_domain_time ON posts(subreddit, time_filter, created_utc);
CREATE INDEX idx_posts_category_popularity ON posts(category_id, popularity_score DESC);

CREATE INDEX idx_sentiment_post_type ON sentiment_analysis(post_id, sentiment_type);
CREATE INDEX idx_sentiment_score ON sentiment_analysis(sentiment_score);
CREATE INDEX idx_sentiment_label ON sentiment_analysis(sentiment_label);
CREATE INDEX idx_sentiment_metadata ON sentiment_analysis USING GIN(metadata);

CREATE INDEX idx_refresh_logs_time_filter ON refresh_logs(time_filter);
CREATE INDEX idx_refresh_logs_domain ON refresh_logs(domain_name);
CREATE INDEX idx_refresh_logs_started_at ON refresh_logs(started_at);
CREATE INDEX idx_refresh_logs_success ON refresh_logs(success);

CREATE INDEX idx_summary_cache_key ON summary_cache(cache_key);
CREATE INDEX idx_summary_cache_expires_at ON summary_cache(expires_at);
CREATE INDEX idx_summary_cache_generated_at ON summary_cache(generated_at);

CREATE INDEX idx_dashboard_cache_expires_at ON dashboard_cache(expires_at);