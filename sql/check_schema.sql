IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reddit') EXEC('CREATE SCHEMA reddit');
IF OBJECT_ID('reddit.submissions','U') IS NULL
BEGIN
  CREATE TABLE reddit.submissions (
    id VARCHAR(20) PRIMARY KEY,
    subreddit NVARCHAR(100) NOT NULL,
    author NVARCHAR(100) NULL,
    created_utc BIGINT NOT NULL,
    is_self BIT NOT NULL,
    url NVARCHAR(2048) NULL,
    permalink NVARCHAR(2048) NOT NULL,
    first_seen_utc BIGINT NOT NULL,
    last_seen_utc BIGINT NOT NULL,
    title NVARCHAR(MAX) NULL,
    selftext NVARCHAR(MAX) NULL,
    score INT NULL,
    upvote_ratio DECIMAL(5,4) NULL,
    num_comments INT NULL,
    edited BIT NULL,
    link_flair_text NVARCHAR(200) NULL,
    removed_by_category NVARCHAR(100) NULL,
    distinguished NVARCHAR(50) NULL,
    locked BIT NULL,
    stickied BIT NULL
  );
END;
IF OBJECT_ID('reddit.submission_snapshots','U') IS NULL
BEGIN
  CREATE TABLE reddit.submission_snapshots (
    snapshot_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    submission_id VARCHAR(20) NOT NULL,
    captured_utc BIGINT NOT NULL,
    title NVARCHAR(MAX) NULL,
    selftext NVARCHAR(MAX) NULL,
    score INT NULL,
    upvote_ratio DECIMAL(5,4) NULL,
    num_comments INT NULL,
    edited BIT NULL,
    link_flair_text NVARCHAR(200) NULL,
    removed_by_category NVARCHAR(100) NULL,
    distinguished NVARCHAR(50) NULL,
    locked BIT NULL,
    stickied BIT NULL,
    content_hash CHAR(64) NOT NULL,
    CONSTRAINT FK_snapshots_submission
      FOREIGN KEY (submission_id) REFERENCES reddit.submissions(id)
  );
END;
