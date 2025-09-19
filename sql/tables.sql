USE AlphaPrismDB

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reddit')
    EXEC('CREATE SCHEMA reddit');
GO

-- Latest state of each submission
IF OBJECT_ID('reddit.submissions','U') IS NULL
BEGIN
CREATE TABLE reddit.submissions (
  id                VARCHAR(20)  NOT NULL PRIMARY KEY,   -- base36 id
  subreddit         NVARCHAR(100) NOT NULL,
  author            NVARCHAR(100) NULL,
  created_utc       BIGINT NOT NULL,
  is_self           BIT NOT NULL,
  url               NVARCHAR(2048) NULL,
  permalink         NVARCHAR(2048) NOT NULL,
  first_seen_utc    BIGINT NOT NULL,
  last_seen_utc     BIGINT NOT NULL,
  title             NVARCHAR(MAX) NULL,
  selftext          NVARCHAR(MAX) NULL,
  score             INT NULL,
  upvote_ratio      DECIMAL(5,4) NULL,                   -- e.g., 0.95
  num_comments      INT NULL,
  edited            BIT NULL,
  link_flair_text   NVARCHAR(200) NULL,
  removed_by_category NVARCHAR(100) NULL,
  distinguished     NVARCHAR(50) NULL,
  locked            BIT NULL,
  stickied          BIT NULL
);
END
GO

IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_submissions_subreddit')
    CREATE INDEX IX_submissions_subreddit ON reddit.submissions(subreddit);
GO
IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_submissions_last_seen')
    CREATE INDEX IX_submissions_last_seen ON reddit.submissions(last_seen_utc);
GO

-- Append-only snapshots
IF OBJECT_ID('reddit.submission_snapshots','U') IS NULL
BEGIN
CREATE TABLE reddit.submission_snapshots (
  snapshot_id       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  submission_id     VARCHAR(20) NOT NULL,
  captured_utc      BIGINT NOT NULL,
  title             NVARCHAR(MAX) NULL,
  selftext          NVARCHAR(MAX) NULL,
  score             INT NULL,
  upvote_ratio      DECIMAL(5,4) NULL,
  num_comments      INT NULL,
  edited            BIT NULL,
  link_flair_text   NVARCHAR(200) NULL,
  removed_by_category NVARCHAR(100) NULL,
  distinguished     NVARCHAR(50) NULL,
  locked            BIT NULL,
  stickied          BIT NULL,
  content_hash      CHAR(64) NOT NULL,                   -- SHA-256 hex
  CONSTRAINT FK_snapshots_submission
    FOREIGN KEY (submission_id) REFERENCES reddit.submissions(id)
);
END
GO

-- Prevent identical duplicate snapshots for the same post
IF NOT EXISTS (SELECT name FROM sys.indexes 
               WHERE name = 'UX_snapshots_submission_hash'
                 AND object_id = OBJECT_ID('reddit.submission_snapshots'))
BEGIN
    CREATE UNIQUE INDEX UX_snapshots_submission_hash
      ON reddit.submission_snapshots(submission_id, content_hash);
END
GO

IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_snapshots_submission_time')
    CREATE INDEX IX_snapshots_submission_time
      ON reddit.submission_snapshots(submission_id, captured_utc);
GO
