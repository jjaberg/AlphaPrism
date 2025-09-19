import curses, time, psutil, threading, queue, logging, os
from collections import defaultdict
from collections import deque

thread_name_registry = {}

# ---------- Logging setup ----------
class CursesListHandler(logging.Handler):
    """Push formatted log messages (with level) into a list for the curses UI."""
    def __init__(self, messages_list, maxlen=600):
        super().__init__()
        self.messages_list = messages_list
        self.maxlen = maxlen

    def emit(self, record):
        msg = self.format(record)
        self.messages_list.append((record.levelno, msg))
        if len(self.messages_list) > self.maxlen:
            self.messages_list.pop(0)

def setup_logger(messages_list):
    logger = logging.getLogger("TUI")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s - %(levelname)-10s - %(threadName)-10s - %(message)s"
    )

    # File output
    fh = logging.FileHandler("./logs/program.log", mode="a")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Curses panel output
    ch = CursesListHandler(messages_list)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger

# ---------- Worker thread (your main program placeholder) ----------
def resolve_thread_name_from_tid(tid, cache, logger=None):
    """
    Resolve a friendly thread name for an OS thread id (tid).
    - cache: your thread_name_registry (dict {tid->name})
    Strategy:
      1) Use cache if present
      2) Scan threading.enumerate() and match native_id
      3) As a last resort, if no native_id match, try ident (some platforms)
    """
    name = cache.get(tid)
    if name:
        return name

    for t in threading.enumerate():
        nid = getattr(t, "native_id", None)
        if nid == tid:
            cache[tid] = t.name
            return t.name

    # Fallback (rare): map by ident if platform equates ident with psutil's tid
    for t in threading.enumerate():
        if t.ident == tid:
            cache[tid] = t.name
            return t.name

    # Couldn’t resolve — leave a hint once
    if logger and cache.get(("__miss__", tid)) is None:
        cache[("__miss__", tid)] = True
        logger.debug(f"[thread-name-resolver] Unmatched psutil tid={tid}")
    return f"tid-{tid}"


def start_named_thread(target, name, *targs, **tkwargs):
    """Create, name, start, and register a thread by native_id (or ident)."""
    t = threading.Thread(target=target, args=targs, kwargs=tkwargs, name=name, daemon=True)
    t.start()

    def _register_id(th=t, nm=name):
        # wait until the thread has OS identifiers
        for _ in range(1000):  # up to ~10s
            nid = getattr(th, "native_id", None)
            if nid is not None:
                thread_name_registry[nid] = nm
                # also cache ident as a fallback mapping
                if th.ident is not None:
                    thread_name_registry[th.ident] = nm
                return
            time.sleep(0.01)
        # fallback: record ident only
        if th.ident is not None:
            thread_name_registry[th.ident] = nm

    threading.Thread(target=_register_id, daemon=True).start()
    return t



def worker_thread(cmd_queue, storage_list, logger):
    logger.info("Worker thread started.")
    while True:
        cmd = cmd_queue.get()
        if cmd == "__exit__":
            logger.info("Worker thread exiting.")
            break
        storage_list.append(cmd)
        logger.info(f"Worker stored: {cmd}")

def counter_worker(logger, interval=0.1):
    i = 1
    logger.info("Counter worker started.")
    while True:
        logger.info(f"Counter: {i}")
        i += 1
        time.sleep(interval)

def hello_worker(logger, interval=10):
    logger.info("Hello worker started.")
    while True:
        logger.info("Hello")
        time.sleep(interval)


# ---------- Helpers ----------
def human_duration(seconds: float) -> str:
    seconds = int(seconds)
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d {h:02d}:{m:02d}:{s:02d}"
    return f"{h:02d}:{m:02d}:{s:02d}"

def human_duration_short(seconds: float) -> str:
    seconds = int(seconds)
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d: return f"{d}d{h:02d}h"
    if h: return f"{h:02d}:{m:02d}h"
    if m: return f"{m:02d}:{s:02d}m"
    return f"{s:02d}s"


def color_for_level(levelno):
    import logging as _logging
    if levelno >= _logging.ERROR:
        return 3   # red
    elif levelno >= _logging.WARNING:
        return 2   # yellow
    else:
        return 1   # green

def color_for_percent(pct):
    if pct >= 90:
        return 3   # red
    elif pct >= 70:
        return 2   # yellow
    else:
        return 1   # green

def bytes_to_human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024.0:
            return f"{n:,.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"

def rate_to_human(bps):
    # bytes per second -> show with /s
    for unit in ("B/s", "KB/s", "MB/s", "GB/s"):
        if bps < 1024.0:
            return f"{bps:,.1f}{unit}"
        bps /= 1024.0
    return f"{bps:.1f}TB/s"

def safe_addstr(win, y, x, text, attr=0, maxw=None):
    if maxw is not None:
        text = text[:maxw]
    try:
        win.addstr(y, x, text, attr)
    except curses.error:
        pass

def draw_title(win, y, text, width):
    safe_addstr(win, y, 0, text[:max(0, width-1)], curses.A_BOLD)
    try:
        win.hline(y+1, 0, curses.ACS_HLINE, max(0, width-1))
    except curses.error:
        pass
    return y + 2  # next content row

def top_processes(limit_cpu=3, limit_mem=3):
    items = []
    for p in psutil.process_iter(attrs=("pid", "name")):
        try:
            cpu = p.cpu_percent(interval=None)        # may be 0.0 on first pass
            mem = p.memory_percent()                  # may raise on race
            items.append({
                "name": p.info.get("name") or f"pid-{p.info.get('pid')}",
                "pid": p.info.get("pid"),
                "cpu": float(cpu or 0.0),
                "mem": float(mem or 0.0),
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    top_cpu = sorted(items, key=lambda d: d["cpu"], reverse=True)[:limit_cpu]
    top_mem = sorted(items, key=lambda d: d["mem"], reverse=True)[:limit_mem]
    return top_cpu, top_mem


# ---------- Main UI ----------
def main(stdscr):
    curses.curs_set(1)
    stdscr.nodelay(True)
    curses.echo()

    curses.start_color()
    curses.init_pair(1, curses.COLOR_GREEN,  curses.COLOR_BLACK)  # INFO / low usage
    curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)  # WARNING / medium
    curses.init_pair(3, curses.COLOR_RED,    curses.COLOR_BLACK)  # ERROR / high
    green = curses.color_pair(1)
    yellow = curses.color_pair(2)
    red = curses.color_pair(3)

    start = time.time()
    cmd_buffer = ""
    log_messages = []          # list of (levelno, message)
    logger = setup_logger(log_messages)

    # ---- Thread monitor state ----
    proc = psutil.Process()  # this running process
    last_thread_times = {}  # { tid -> cpu_time_seconds(user+system) }
    thread_first_seen = {}  # { tid -> timestamp }
    recently_ended = deque(maxlen=20)  # list of (name, tid, ended_at)

    # worker setup
    cmd_queue = queue.Queue()
    storage_list = []

    main_tid = getattr(threading.current_thread(), "native_id", None) or threading.get_ident()
    thread_name_registry[main_tid] = "main"

    worker = start_named_thread(worker_thread, "worker", cmd_queue, storage_list, logger)
    counter = start_named_thread(counter_worker, "counter", logger)
    hello = start_named_thread(hello_worker, "hello", logger)

    for t in threading.enumerate():
        logger.debug(f"thread enumerate -> name={t.name} ident={t.ident} native_id={getattr(t, 'native_id', None)}")

    # prime per-core cpu measurement so next reads are meaningful
    _ = psutil.cpu_percent(percpu=True)

    # net/disk rate baseline
    prev_time = time.time()
    prev_net = psutil.net_io_counters(pernic=True)
    prev_disk = psutil.disk_io_counters()

    logger.info("Program started.")

    while True:
        h, w = stdscr.getmaxyx()

        # Layout
        # --- layout sizes ---
        header_h = 5
        input_h = 2
        middle_h = max(8, h - header_h - input_h)

        # vitals = 1/3 width, logs = 2/3 width
        vitals_w = max(20, w // 3)
        log_w = w - vitals_w  # == 2/3 (rounded)

        # windows
        header = curses.newwin(header_h, w, 0, 0)
        vitals = curses.newwin(middle_h, vitals_w, header_h, 0)
        logw = curses.newwin(middle_h, log_w, header_h, vitals_w)
        inputw = curses.newwin(input_h, w, header_h + middle_h, 0)

        # ---------- HEADER ----------
        prog_uptime = time.time() - start
        try:
            machine_uptime = time.time() - psutil.boot_time()
        except Exception:
            machine_uptime = 0.0

        host = os.uname().nodename if hasattr(os, "uname") else "host"
        header_text = (
            f"Uptime P:{human_duration(prog_uptime)} | M:{human_duration(machine_uptime)}  "
            f"Host: {host}  Status: OK"
        )

        # top bar
        try:
            header.hline(0, 0, curses.ACS_HLINE, w - 1)
        except curses.error:
            pass

        # header text in middle row
        safe_addstr(header, 1, 0, 'AlphaPrism', curses.A_BOLD)
        safe_addstr(header, 2, 0, header_text[:w - 1], curses.A_BOLD)

        # bottom bar
        try:
            header.hline(4, 0, curses.ACS_HLINE, w - 1)
        except curses.error:
            pass

        # ---------- COLLECT METRICS ----------
        # CPU loads
        try:
            la1, la5, la15 = os.getloadavg()
        except Exception:
            la1 = la5 = la15 = 0.0

        # Per-core CPU
        per_core = psutil.cpu_percent(percpu=True)  # non-blocking since primed
        overall_cpu = sum(per_core)/max(1,len(per_core))

        # Memory & swap
        vm = psutil.virtual_memory()
        sm = psutil.swap_memory()

        # Network + Disk rates
        now = time.time()
        dt = max(0.001, now - prev_time)
        cur_net = psutil.net_io_counters(pernic=True)
        cur_disk = psutil.disk_io_counters()

        nic_rates = {}
        for nic, stats in cur_net.items():
            pstats = prev_net.get(nic)
            if not pstats:
                continue
            rx = max(0, stats.bytes_recv - pstats.bytes_recv) / dt
            tx = max(0, stats.bytes_sent - pstats.bytes_sent) / dt
            rx_err = max(0, stats.errin - pstats.errin) / dt
            tx_err = max(0, stats.errout - pstats.errout) / dt
            rx_drop = max(0, stats.dropin - pstats.dropin) / dt
            tx_drop = max(0, stats.dropout - pstats.dropout) / dt
            nic_rates[nic] = dict(rx=rx, tx=tx, rx_err=rx_err, tx_err=tx_err, rx_drop=rx_drop, tx_drop=tx_drop,
                                  rx_total=stats.bytes_recv, tx_total=stats.bytes_sent)

        # choose "active" NIC by highest combined rate
        active_nic = None
        if nic_rates:
            active_nic = max(nic_rates.items(), key=lambda kv: kv[1]["rx"]+kv[1]["tx"])[0]

        # Disk rate (overall)
        d_read_bps  = max(0, cur_disk.read_bytes  - prev_disk.read_bytes)  / dt if cur_disk and prev_disk else 0
        d_write_bps = max(0, cur_disk.write_bytes - prev_disk.write_bytes) / dt if cur_disk and prev_disk else 0

        prev_time = now
        prev_net = cur_net
        prev_disk = cur_disk

        # Processes
        try:
            procs_total = len(psutil.pids())
        except Exception:
            procs_total = 0
        top_cpu, top_mem = top_processes(limit_cpu=3, limit_mem=3)

        # Temperatures
        temps = {}
        try:
            tmap = psutil.sensors_temperatures(fahrenheit=False)
            for chip, entries in tmap.items():
                for t in entries:
                    # pick CPU-ish labels
                    if any(k in (t.label or chip).lower() for k in ("cpu","package","core","k10temp","acpitz","coretemp","zen")):
                        temps[(t.label or chip)] = t.current
        except (AttributeError, Exception):
            pass

        # ---- Thread metrics (per OS thread) ----
        # Map OS native_id -> Python thread object
        py_threads_by_native = {}
        for t in threading.enumerate():
            # native_id is available on Python 3.8+. Fall back to ident if needed.
            tid = getattr(t, "native_id", None) or t.ident
            if tid is not None:
                py_threads_by_native[tid] = t

        # Read psutil's per-thread CPU times
        cur_threads = {}
        for th in proc.threads():  # each has .id, .user_time, .system_time
            tid = th.id
            cpu_time = float(th.user_time + th.system_time)
            cur_threads[tid] = cpu_time

        now_ts = time.time()
        dt = max(0.001, now_ts - prev_time)  # you already have prev_time for rates

        thread_rows = []  # will render in vitals

        # Detect ended threads (present last loop but missing now)
        ended_tids = set(last_thread_times.keys()) - set(cur_threads.keys())
        for tid in ended_tids:
            # Try to remember the last known name; if we still have the Python thread obj it will be dead.
            name = None
            for t in list(py_threads_by_native.values()):
                if (getattr(t, "native_id", None) or t.ident) == tid:
                    name = t.name
                    break
            if name is None:
                name = f"tid-{tid}"
            recently_ended.appendleft((name, tid, now_ts))
            last_thread_times.pop(tid, None)
            thread_first_seen.pop(tid, None)

        # Build rows for currently alive psutil threads
        for tid, cpu_time in cur_threads.items():
            prev = last_thread_times.get(tid, cpu_time)
            delta = max(0.0, cpu_time - prev)
            # % of a single core during this dt:
            cpu_pct = (delta / dt) * 100.0

            name = resolve_thread_name_from_tid(tid, thread_name_registry, logger)

            daemon = getattr(t, "daemon", False)
            first_seen = thread_first_seen.setdefault(tid, now_ts)
            age = now_ts - first_seen

            thread_rows.append({
                "name": name,
                "tid": tid,
                "daemon": daemon,
                "cpu_pct": cpu_pct,
                "cpu_time": cpu_time,
                "age": age,
                "alive": True,
            })

        # Persist current times for next loop
        last_thread_times = cur_threads

        # ---------- VITALS RENDER ----------
        row = 0
        row = draw_title(vitals, row, "CPU", vitals_w)
        # Load avg + overall %
        safe_addstr(vitals, row, 0, f"Load avg: {la1:.2f} {la5:.2f} {la15:.2f}")
        safe_addstr(vitals, row, 26, "Overall: ")
        cpair = curses.color_pair(color_for_percent(overall_cpu))
        safe_addstr(vitals, row, 35, f"{overall_cpu:5.1f}%", cpair)
        row += 1

        # Per-core percentages, each on its own line
        for idx, pct in enumerate(per_core):
            label = f"Core {idx:02d}: {pct:5.1f}%"
            cpair = curses.color_pair(color_for_percent(pct))
            safe_addstr(vitals, row, 0, label, cpair)
            row += 1
        row += 1  # spacing after the block

        # Memory
        row = draw_title(vitals, row, "Memory", vitals_w)
        safe_addstr(vitals, row, 0,  f"RAM:  Used {bytes_to_human(vm.used):>8} / {bytes_to_human(vm.total):<8}  ")
        safe_addstr(vitals, row, 48, "Util: ")
        safe_addstr(vitals, row, 54, f"{vm.percent:5.1f}%", curses.color_pair(color_for_percent(vm.percent)))
        row += 1
        safe_addstr(vitals, row, 0,  f"Swap: Used {bytes_to_human(sm.used):>8} / {bytes_to_human(sm.total):<8}  ")
        safe_addstr(vitals, row, 48, "Util: ")
        safe_addstr(vitals, row, 54, f"{sm.percent:5.1f}%", curses.color_pair(color_for_percent(sm.percent)))
        row += 2

        # Networking (active NIC + totals + errors/drops)
        row = draw_title(vitals, row, "Networking", vitals_w)
        if active_nic:
            n = nic_rates[active_nic]
            safe_addstr(vitals, row, 0,  f"NIC: {active_nic}")
            row += 1
            safe_addstr(vitals, row, 0,  f"RX: {rate_to_human(n['rx'])}   TX: {rate_to_human(n['tx'])}")
            row += 1
            safe_addstr(vitals, row, 0,  f"RX total: {bytes_to_human(n['rx_total'])}   TX total: {bytes_to_human(n['tx_total'])}")
            row += 1
            errs = f"Err: in {n['rx_err']:.2f}/s out {n['tx_err']:.2f}/s   Drop: in {n['rx_drop']:.2f}/s out {n['tx_drop']:.2f}/s"
            safe_addstr(vitals, row, 0, errs)
            row += 1
        else:
            safe_addstr(vitals, row, 0, "No active NIC detected")
            row += 1

        # Disk I/O (brief, since disk usage % is already in header earlier versions)
        row = draw_title(vitals, row, "Disk I/O", vitals_w)
        safe_addstr(vitals, row, 0, f"Read: {rate_to_human(d_read_bps)}   Write: {rate_to_human(d_write_bps)}")
        row += 2

        # Processes
        row = draw_title(vitals, row, f"Processes (total: {procs_total})", vitals_w)
        safe_addstr(vitals, row, 0, "Top CPU:")
        row += 1
        for p in top_cpu:
            safe_addstr(vitals, row, 0, f"  {p['name'][:18]:18}  PID {p['pid']}  CPU {p['cpu']:4.1f}%")
            row += 1
        safe_addstr(vitals, row, 0, "Top MEM:")
        row += 1
        for p in top_mem:
            safe_addstr(vitals, row, 0, f"  {p['name'][:18]:18}  PID {p['pid']}  MEM {p['mem']:4.1f}%")
            row += 1
        row += 1

        # Temperatures
        row = draw_title(vitals, row, "Temperatures", vitals_w)
        if temps:
            for label, tval in list(temps.items())[:min(6, len(temps))]:
                col = curses.color_pair(1 if tval < 70 else 2 if tval < 85 else 3)
                safe_addstr(vitals, row, 0, f"{label[:18]:18}: {tval:4.1f}°C", col)
                row += 1
        else:
            safe_addstr(vitals, row, 0, "No temperature sensors available")
            row += 1

        # ---------- THREADS ----------
        row = draw_title(vitals, row, "Threads (alive)", vitals_w)

        # Sort by CPU descending, show top N (fit to the pane)
        # Each line: name (padded), tid, D/Y flag, CPU%, CPU time, age
        visible_thread_lines = max(3, min(10, middle_h // 2))
        thread_rows_sorted = sorted(thread_rows, key=lambda r: r["cpu_pct"], reverse=True)[:visible_thread_lines]

        for rinfo in thread_rows_sorted:
            lvl_color = curses.color_pair(color_for_percent(min(99.9, rinfo["cpu_pct"])))
            line = (
                f"{rinfo['name'][:12]:12} "
                f"tid:{rinfo['tid']:>6} "
                f"{'D' if rinfo['daemon'] else ' '}/A "
                f"CPU:{rinfo['cpu_pct']:5.1f}% "
                f"time:{rinfo['cpu_time']:6.2f}s "
                f"age:{human_duration_short(rinfo['age']):>6}"
            )
            safe_addstr(vitals, row, 0, line[:vitals_w - 1], lvl_color)
            row += 1

        # Recently ended (short-lived notice)
        # Show any that ended within the last ~10 seconds
        ended_cutoff = now_ts - 10.0
        recent = [e for e in list(recently_ended) if e[2] >= ended_cutoff]
        if recent:
            row = draw_title(vitals, row, "Threads (ended last 10s)", vitals_w)
            for name, tid, tstamp in recent[:3]:
                since = human_duration_short(now_ts - tstamp)
                safe_addstr(vitals, row, 0, f"{name[:12]:12} tid:{tid:>6} ended {since} ago", curses.color_pair(2))
                row += 1

        # ---------- LOGS (right half width) ----------
        safe_addstr(logw, 0, 0, "Logs", curses.A_BOLD)
        try:
            logw.hline(1, 0, curses.ACS_HLINE, max(0, log_w-1))
        except curses.error:
            pass
        visible_lines = max(1, middle_h - 2)
        last_msgs = log_messages[-visible_lines:]
        for i, (lvl, msg) in enumerate(last_msgs):
            color = curses.color_pair(color_for_level(lvl))
            safe_addstr(logw, i+2, 0, msg, color, maxw=log_w-1)

        # ---------- INPUT ----------
        safe_addstr(inputw, 0, 0, "-" * max(0, w-1))
        safe_addstr(inputw, 1, 0, "Command> " + cmd_buffer)

        # Refresh
        header.refresh(); vitals.refresh(); logw.refresh(); inputw.refresh()

        # ---------- Input handling ----------
        try:
            ch = stdscr.getch()
        except curses.error:
            ch = -1

        if ch == -1:
            time.sleep(0.2)
            continue

        if ch in (curses.KEY_ENTER, 10, 13):
            cmd = cmd_buffer.strip()
            if cmd:
                if cmd == "exit":
                    logger.info("Exit command received.")
                    cmd_queue.put("__exit__")
                    return
                elif cmd.startswith("echo "):
                    text = cmd[5:]
                    cmd_queue.put(text)             # send to worker
                    logger.info(f"Echo sent: {text}")
                else:
                    logger.warning(f"Unknown command: {cmd}")
            cmd_buffer = ""
        elif ch in (curses.KEY_BACKSPACE, 127, 8):
            cmd_buffer = cmd_buffer[:-1]
        elif 0 <= ch < 256:
            cmd_buffer += chr(ch)

        time.sleep(0.05)

if __name__ == "__main__":
    curses.wrapper(main)
