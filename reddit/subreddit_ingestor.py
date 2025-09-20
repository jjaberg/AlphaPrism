import os
import time
import hashlib
import threading
import logging
from typing import Optional, Callable, Iterable, Literal

import pyodbc
import praw
from dotenv import load_dotenv

Listing = Literal["new", "hot", "top", "best", "rising", "controversial"]
TimeFilter = Literal["hour", "day", "week", "month", "year", "all"]


class SubredditIngestConfig:
    def __init__(
        self,
        subreddit: str,
        limit: int = 200,
        listing: Listing = "new",
        time_filter: TimeFilter = "day",
        stream_after: bool = False,
        hot_refresh_interval: int = 60,
        hot_refresh_count: int = 100,
        sleep_between_items: float = 0.2,
        heartbeat_interval: int = 30,
        name: Optional[str] = None,
    ):
        self.subreddit = subreddit
        self.limit = limit
        self.listing = listing
        self.time_filter = time_filter
        self.stream_after = stream_after
        self.hot_refresh_interval = hot_refresh_interval
        self.hot_refresh_count = hot_refresh_count
        self.sleep_between_items = sleep_between_items
        self.heartbeat_interval = heartbeat_interval
        self.name = name or f"SubredditIngestor-{subreddit}-{listing}"

class SubredditIngestor(threading.Thread):
    """
    Threadable worker that ingests subreddit submissions into MSSQL and optionally streams.
    Accepts an external logger from your main program.
    """

    def __init__(
        self,
        config: SubredditIngestConfig,
        reddit_factory: Optional[Callable[[], praw.Reddit]] = None,
        conn_factory: Optional[Callable[[], pyodbc.Connection]] = None,
        logger: Optional[logging.Logger] = None,
        daemon: bool = True,
    ):
        super().__init__(name=config.name, daemon=daemon)
        self.config = config
        self._stop_event = threading.Event()

        # DI
        self._reddit_factory = reddit_factory or self._default_reddit_factory
        self._conn_factory = conn_factory or self._default_conn_factory

        self._reddit: Optional[praw.Reddit] = None
        self._conn: Optional[pyodbc.Connection] = None

        # Logger
        self._log = logger or logging.getLogger(f"{__name__}.{self.name}")

        # Heartbeat state
        self._last_heartbeat = 0.0
        self._processed_total = 0
        self._snapshots_inserted = 0
        self._snapshots_deduped = 0

        # SQL
        self._UPSERT_SUBMISSION_SQL = """
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
        self._INSERT_SNAPSHOT_IF_NEW_SQL = """
        INSERT INTO reddit.submission_snapshots
        (submission_id, captured_utc, title, selftext, score, upvote_ratio, num_comments,
         edited, link_flair_text, removed_by_category, distinguished, locked, stickied, content_hash)
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM reddit.submission_snapshots
            WHERE submission_id = ? AND content_hash = ?
        );
        """

    # ---- lifecycle ----
    def stop(self):
        self._log.info("Stop requested for %s", self.name)
        self._stop_event.set()

    def run(self):
        self._log.info(
            "Starting worker: subreddit=%s listing=%s limit=%s stream_after=%s",
            self.config.subreddit, self.config.listing, self.config.limit, self.config.stream_after
        )
        try:
            # Robust startup: retry connecting to Reddit/MSSQL before giving up
            backoff = 1.0
            while not self._stop_event.is_set():
                try:
                    self._reddit = self._reddit_factory()
                    self._conn = self._conn_factory()
                    break
                except Exception:
                    self._log.exception("Startup dependency error (retrying in %.1fs)", backoff)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 30)

            if self._reddit is None or self._conn is None:
                self._log.error("Startup aborted due to stop request before dependencies ready")
                return

            self._log.debug("Dependencies ready (praw + MSSQL connection established)")

            self._initial_ingest()

            self._log.info("Initial ingest complete; stream_after=%s", self.config.stream_after)

            if self.config.stream_after and not self._stop_event.is_set():
                self._log.info("Dispatching into stream loop…")
                self._stream_loop()

        except Exception:
            self._log.exception("Fatal error in worker %s", self.name)
        finally:
            try:
                if self._conn:
                    self._conn.close()
                    self._log.debug("DB connection closed")
            except Exception:
                self._log.exception("Error while closing DB connection")
            self._log.info("Worker %s exited", self.name)

    @staticmethod
    def _default_reddit_factory() -> praw.Reddit:
        load_dotenv()
        reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.environ.get("REDDIT_USER_AGENT", "AlphaPrism"),
            ratelimit_seconds=5,
        )
        reddit.read_only = True
        return reddit

    @staticmethod
    def _default_conn_factory() -> pyodbc.Connection:
        return pyodbc.connect(os.getenv("MSSQL_CONN_STR"), autocommit=False)

    # ---- ingest logic ----
    def _initial_ingest(self):
        sub = self._reddit.subreddit(self.config.subreddit)
        count = 0
        for submission in self._iter_listing(sub):
            if self._stop_event.is_set():
                self._log.info("Initial ingest stopped early at %d items", count)
                break
            self._process_submission(submission)
            count += 1
            if count % 50 == 0:
                self._log.debug("Initial ingest progress: %d/%d", count, self.config.limit)
            time.sleep(self.config.sleep_between_items)
            self._maybe_heartbeat()
        self._log.info("Initial ingest done: %d items processed", count)

    def _stream_loop(self):
        sub = self._reddit.subreddit(self.config.subreddit)
        # Non-blocking stream so we can keep doing heartbeats and hot refreshes
        stream = sub.stream.submissions(skip_existing=True, pause_after=0)
        last_hot = 0.0
        self._log.info("Entering stream loop for r/%s", self.config.subreddit)
        self._log.debug("Stream handle ready (skip_existing=True, pause_after=0)")

        while not self._stop_event.is_set():
            self._maybe_heartbeat()
            now = time.time()

            # Periodic hot refresh (score/comment drift)
            if now - last_hot >= self.config.hot_refresh_interval:
                try:
                    self._log.debug("Hot refresh: top %d", self.config.hot_refresh_count)
                    for submission in sub.hot(limit=self.config.hot_refresh_count):
                        if self._stop_event.is_set():
                            break
                        self._process_submission(submission)
                        time.sleep(self.config.sleep_between_items)
                except Exception:
                    self._log.exception("Error during hot refresh")
                finally:
                    last_hot = 0.0

            # Drain any new items from the stream; yields None when caught up
            got_any = False
            try:
                for submission in stream:
                    if submission is None:
                        break  # no new items right now
                    got_any = True
                    self._process_submission(submission)
            except Exception:
                self._log.exception("Stream error; continuing")
                time.sleep(1.0)

            if not got_any:
                # avoid a tight loop when idle
                time.sleep(0.5)


    def _iter_listing(self, sub) -> Iterable:
        l = self.config.listing.lower()
        tf = self.config.time_filter
        limit = self.config.limit

        valid = {"new", "hot", "top", "best", "rising", "controversial"}
        if l not in valid:
            raise ValueError(f"listing must be one of {sorted(valid)}")
        if l in {"top", "controversial", "best"}:
            if tf not in {"hour", "day", "week", "month", "year", "all"}:
                raise ValueError("invalid time_filter")

        if l == "new":
            return sub.new(limit=limit)
        if l == "hot":
            return sub.hot(limit=limit)
        if l == "rising":
            return sub.rising(limit=limit)
        if l == "top":
            return sub.top(limit=limit, time_filter=tf)
        if l == "controversial":
            return sub.controversial(limit=limit, time_filter=tf)
        if l == "best":
            try:
                return sub.best(limit=limit, time_filter=tf)
            except TypeError:
                return sub.best(limit=limit)
        raise RuntimeError("unreachable")

    def _maybe_heartbeat(self):
        now = time.time()
        if self._last_heartbeat == 0.0:
            self._last_heartbeat = now
            return
        if now - self._last_heartbeat >= self.config.heartbeat_interval:
            self._log.debug(
                "HB r/%s listing=%s processed_total=%d snapshots: +%d (deduped %d)",
                self.config.subreddit,
                self.config.listing,
                self._processed_total,
                self._snapshots_inserted,
                self._snapshots_deduped,
            )
            # reset per-interval counters; keep totals
            self._snapshots_inserted = 0
            self._snapshots_deduped = 0
            self._last_heartbeat = now
    
    # ---- persistence ----
    @staticmethod
    def _now_utc_int() -> int:
        return int(time.time())

    @staticmethod
    def _b(v: bool) -> int:
        return 1 if v else 0

    def _row_from_submission(self, s):
        return {
            "id": s.id,
            "subreddit": str(s.subreddit),
            "author": str(s.author) if s.author else None,
            "created_utc": int(s.created_utc),
            "is_self": self._b(s.is_self),
            "url": s.url,
            "permalink": f"https://www.reddit.com{s.permalink}",
            "title": s.title or "",
            "selftext": s.selftext or "",
            "score": int(s.score) if s.score is not None else None,
            "upvote_ratio": float(s.upvote_ratio) if s.upvote_ratio is not None else None,
            "num_comments": int(s.num_comments) if s.num_comments is not None else None,
            "edited": self._b(bool(s.edited)),
            "link_flair_text": s.link_flair_text,
            "removed_by_category": getattr(s, "removed_by_category", None),
            "distinguished": s.distinguished,
            "locked": self._b(getattr(s, "locked", False)),
            "stickied": self._b(getattr(s, "stickied", False)),
        }

    @staticmethod
    def _snapshot_hash(d: dict) -> str:
        parts = (
            d["title"], d["selftext"], str(d["score"]), str(d["upvote_ratio"]),
            str(d["num_comments"]), str(d["edited"]), str(d["link_flair_text"]),
            str(d["removed_by_category"]), str(d["distinguished"]),
            str(d["locked"]), str(d["stickied"]),
        )
        return hashlib.sha256("||".join(x if x is not None else "∅" for x in parts).encode("utf-8")).hexdigest()

    def _process_submission(self, s):
        d = self._row_from_submission(s)
        self._upsert_submission(d)
        inserted = self._add_snapshot(s.id, d)
        self._processed_total += 1
        if inserted:
            self._snapshots_inserted += 1
        else:
            self._snapshots_deduped += 1
        self._maybe_heartbeat()
        self._log.debug(
            "Upserted %s (score=%s, comments=%s)%s",
            d["id"], d["score"], d["num_comments"],
            "" if inserted else " [snapshot deduped]"
        )

    def _upsert_submission(self, d: dict):
        ts = self._now_utc_int()
        vals = (
            d["id"], d["subreddit"], d["author"], d["created_utc"], d["is_self"],
            d["url"], d["permalink"], ts, ts, d["title"], d["selftext"], d["score"],
            d["upvote_ratio"], d["num_comments"], d["edited"], d["link_flair_text"],
            d["removed_by_category"], d["distinguished"], d["locked"], d["stickied"]
        )
        cur = self._conn.cursor()
        cur.execute(self._UPSERT_SUBMISSION_SQL, vals)
        self._conn.commit()

    def _add_snapshot(self, submission_id: str, d: dict) -> bool:
        h = self._snapshot_hash(d)
        cur = self._conn.cursor()
        vals = (
            submission_id, self._now_utc_int(), d["title"], d["selftext"], d["score"],
            d["upvote_ratio"], d["num_comments"], d["edited"], d["link_flair_text"],
            d["removed_by_category"], d["distinguished"], d["locked"], d["stickied"], h,
            submission_id, h
        )
        cur.execute(self._INSERT_SNAPSHOT_IF_NEW_SQL, vals)
        self._conn.commit()
        # rowcount: 1 if inserted, 0 if deduped (note: some drivers return -1; pyodbc usually returns 1/0 here)
        try:
            return cur.rowcount == 1
        except Exception:
            return True  # default optimistic
