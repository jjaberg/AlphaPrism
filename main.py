import os
import time
import hashlib
import pyodbc
import praw
from dotenv import load_dotenv

load_dotenv()

SUBREDDIT = os.getenv("SUBREDDIT", "wallstreetbets")

# MSSQL connection (Docker default port 1433)
MSSQL_CONN_STR = os.getenv(
    "MSSQL_CONN_STR"
)

# Reddit API creds
CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT    = os.getenv("REDDIT_USER_AGENT")

if not (CLIENT_ID and CLIENT_SECRET):
    raise RuntimeError("Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET env vars.")

reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT,
    ratelimit_seconds=5,
)
reddit.read_only = True

def now_utc_int() -> int:
    return int(time.time())

def b(v):  # bool -> bit
    return 1 if v else 0

def row_from_submission(s):
    return {
        "id": s.id,
        "subreddit": str(s.subreddit),
        "author": str(s.author) if s.author else None,
        "created_utc": int(s.created_utc),
        "is_self": b(s.is_self),
        "url": s.url,
        "permalink": f"https://www.reddit.com{s.permalink}",
        "title": s.title or "",
        "selftext": s.selftext or "",
        "score": int(s.score) if s.score is not None else None,
        "upvote_ratio": float(s.upvote_ratio) if s.upvote_ratio is not None else None,
        "num_comments": int(s.num_comments) if s.num_comments is not None else None,
        "edited": b(bool(s.edited)),
        "link_flair_text": s.link_flair_text,
        "removed_by_category": getattr(s, "removed_by_category", None),
        "distinguished": s.distinguished,
        "locked": b(getattr(s, "locked", False)),
        "stickied": b(getattr(s, "stickied", False)),
    }

def snapshot_hash(d) -> str:
    parts = (
        d["title"],
        d["selftext"],
        str(d["score"]),
        str(d["upvote_ratio"]),
        str(d["num_comments"]),
        str(d["edited"]),
        str(d["link_flair_text"]),
        str(d["removed_by_category"]),
        str(d["distinguished"]),
        str(d["locked"]),
        str(d["stickied"]),
    )
    return hashlib.sha256("||".join(x if x is not None else "∅" for x in parts).encode("utf-8")).hexdigest()

UPSERT_SUBMISSION_SQL = """
MERGE reddit.submissions AS target
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS src
      (id, subreddit, author, created_utc, is_self, url, permalink, first_seen_utc, last_seen_utc,
       title, selftext, score, upvote_ratio, num_comments, edited, link_flair_text,
       removed_by_category, distinguished, locked, stickied)
ON (target.id = src.id)
WHEN MATCHED THEN UPDATE SET
    subreddit = src.subreddit,
    author = src.author,
    created_utc = src.created_utc,
    is_self = src.is_self,
    url = src.url,
    permalink = src.permalink,
    last_seen_utc = src.last_seen_utc,
    title = src.title,
    selftext = src.selftext,
    score = src.score,
    upvote_ratio = src.upvote_ratio,
    num_comments = src.num_comments,
    edited = src.edited,
    link_flair_text = src.link_flair_text,
    removed_by_category = src.removed_by_category,
    distinguished = src.distinguished,
    locked = src.locked,
    stickied = src.stickied
WHEN NOT MATCHED BY TARGET THEN
    INSERT (id, subreddit, author, created_utc, is_self, url, permalink, first_seen_utc, last_seen_utc,
            title, selftext, score, upvote_ratio, num_comments, edited, link_flair_text,
            removed_by_category, distinguished, locked, stickied)
    VALUES (src.id, src.subreddit, src.author, src.created_utc, src.is_self, src.url, src.permalink,
            src.first_seen_utc, src.last_seen_utc, src.title, src.selftext, src.score, src.upvote_ratio,
            src.num_comments, src.edited, src.link_flair_text, src.removed_by_category,
            src.distinguished, src.locked, src.stickied);
"""

INSERT_SNAPSHOT_IF_NEW_SQL = """
INSERT INTO reddit.submission_snapshots
(submission_id, captured_utc, title, selftext, score, upvote_ratio, num_comments,
 edited, link_flair_text, removed_by_category, distinguished, locked, stickied, content_hash)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM reddit.submission_snapshots
    WHERE submission_id = ? AND content_hash = ?
);
"""

def init_db(conn, schema_path: str = "schema_mssql.sql"):
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    # split on GO (simple)
    batches = [b.strip() for b in sql.split("GO") if b.strip()]
    cur = conn.cursor()
    for batch in batches:
        cur.execute(batch)
    conn.commit()

def upsert_submission(conn, d):
    ts = now_utc_int()
    vals = (
        d["id"], d["subreddit"], d["author"], d["created_utc"], d["is_self"], d["url"], d["permalink"],
        ts, ts, d["title"], d["selftext"], d["score"], d["upvote_ratio"], d["num_comments"],
        d["edited"], d["link_flair_text"], d["removed_by_category"], d["distinguished"], d["locked"], d["stickied"]
    )
    cur = conn.cursor()
    cur.execute(UPSERT_SUBMISSION_SQL, vals)
    conn.commit()

def add_snapshot(conn, submission_id, d):
    h = snapshot_hash(d)
    cur = conn.cursor()
    vals = (
        submission_id, now_utc_int(), d["title"], d["selftext"], d["score"], d["upvote_ratio"],
        d["num_comments"], d["edited"], d["link_flair_text"], d["removed_by_category"],
        d["distinguished"], d["locked"], d["stickied"], h,
        submission_id, h
    )
    cur.execute(INSERT_SNAPSHOT_IF_NEW_SQL, vals)
    conn.commit()

def process_submission(conn, s):
    d = row_from_submission(s)
    upsert_submission(conn, d)
    add_snapshot(conn, s.id, d)

def initial_backfill(subreddit, conn, limit=1000):
    for s in reddit.subreddit(subreddit).new(limit=limit):
        process_submission(conn, s)

def run_loop(subreddit, conn):
    print("Starting live tracking… Ctrl+C to stop.")
    last_hot_refresh = 0
    HOT_INTERVAL = 60  # seconds
    stream = reddit.subreddit(subreddit).stream.submissions(skip_existing=True)

    while True:
        now = time.time()
        if now - last_hot_refresh > HOT_INTERVAL:
            for s in reddit.subreddit(subreddit).hot(limit=100):
                process_submission(conn, s)
                time.sleep(0.2)
            last_hot_refresh = now

        try:
            s = next(stream)
            process_submission(conn, s)
        except StopIteration:
            time.sleep(1)
        except Exception as e:
            print("Error:", e)
            time.sleep(5)


if __name__ == "__main__":
    
    # Connect
    conn = pyodbc.connect(MSSQL_CONN_STR, autocommit=False)

    # Create schema if needed
    #init_db(conn, "schema_mssql.sql")

    print(f"Backfilling r/{SUBREDDIT} (latest ~1000)…")
    initial_backfill(SUBREDDIT, conn)

    run_loop(SUBREDDIT, conn)
