# ============================================================
# =  CONFIG & LOGGING
# ============================================================
from __future__ import annotations
import re
import glob
import json
import gzip
import requests
import logging
import shutil
import heapq
from collections import defaultdict
from pathlib import Path
from typing import Dict
from datetime import datetime
from tqdm import tqdm

try:
    import orjson
except ImportError:  # pragma: no cover - ускорение сериализации опционально
    orjson = None

JSON_SERIALIZER_NAME = "orjson" if orjson is not None else "json"

OpKey = tuple[int, int, str, str]

# --- Загрузка конфигурации ---
def load_config():
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

config = load_config()

# --- Настройка логирования ---
LOG_DIR = Path(config["paths"]["log_dir"])
LOG_DIR.mkdir(exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"trace-vrs-{timestamp}.log"
replaced_ops_dir = LOG_DIR / f"replaced-active-ops-{timestamp}"
replaced_ops_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# --- Конфигурация ClickHouse ---
CLICKHOUSE_URL = config["clickhouse"]["url"]
CLICKHOUSE_USER = config["clickhouse"]["user"]
CLICKHOUSE_PASSWORD = config["clickhouse"]["password"]
CLICKHOUSE_DATABASE = config["clickhouse"]["database"]

# --- Параметры обработки ---
BATCH_SIZE = config["processing"]["batch_size"]
TIMEOUT = config["processing"]["timeout"]
DATASET_NAME = config["processing"]["dataset_name"]
PROCESS_CLIENT_SERVER_OPS = config["processing"]["client_server_operations"]
PROCESS_BACKGROUND_JOBS = config["processing"]["background_jobs"]
COUNT_LINES_FOR_PROGRESS = config["processing"].get("count_lines_for_progress", True)
INSERT_COMPRESSION = str(config["processing"].get("insert_compression", "gzip")).strip().lower()
if INSERT_COMPRESSION not in {"gzip", "none"}:
    INSERT_COMPRESSION = "gzip"
try:
    GZIP_LEVEL = int(config["processing"].get("gzip_level", 1))
except (TypeError, ValueError):
    GZIP_LEVEL = 1
if GZIP_LEVEL < 1 or GZIP_LEVEL > 9:
    GZIP_LEVEL = 1

# --- Ограничения фильтрации операций ---
MIN_OPERATION_DURATION_MS = config["filtering"]["min_operation_duration_ms"]
MIN_OPERATION_EVENTS = config["filtering"]["min_operation_events"]
try:
    CALL_EXPECTED_WINDOW_SECONDS = int(config["filtering"].get("call_expected_window_seconds", 1))
except (TypeError, ValueError):
    CALL_EXPECTED_WINDOW_SECONDS = 10
if CALL_EXPECTED_WINDOW_SECONDS < 0:
    CALL_EXPECTED_WINDOW_SECONDS = 10
try:
    CALL_BIND_TTL_SECONDS = int(config["filtering"].get("call_bind_ttl_seconds", 3600))
except (TypeError, ValueError):
    CALL_BIND_TTL_SECONDS = 3600
if CALL_BIND_TTL_SECONDS < 1:
    CALL_BIND_TTL_SECONDS = 3600
if CALL_BIND_TTL_SECONDS < CALL_EXPECTED_WINDOW_SECONDS:
    CALL_BIND_TTL_SECONDS = CALL_EXPECTED_WINDOW_SECONDS


# Reuse HTTP connection
SESSION = requests.Session()

# --- Таблицы ClickHouse ---
TABLE_OPERATIONS = "operations"
TABLE_EVENTS = "events"
TABLE_EVENT_STATS = "event_stats"
TABLE_CALL_OPS = "calls"

# --- Компиляция регулярных выражений ---
EVENT_START_RE = re.compile(r"^(\d{2}):(\d{2})\.(\d{6})-(\d+),([A-Za-z]+),")
CONTEXT_PATTERN = re.compile(r',Context=(?:"([^"]*)"|\'([^\']*)\'|([^,\r\n]+))', re.DOTALL)
SESSION_ID_RE = re.compile(r"^\s*(\d+)(?:\(\d+\))?\s*$")
PROP_KEYS = ("t:clientID", "SessionID", "Usr", "t:connectID", "Appl", "Func", "Nmb", "IB", "p:processName")
PROP_KEY_TOKENS = {key: f"{key}=" for key in PROP_KEYS}
ROUTE_KEYS_DEFAULT = frozenset({"t:clientID", "SessionID", "t:connectID", "p:processName"})
ROUTE_KEYS_SESN = frozenset({"Nmb", "Func", "Appl", "t:connectID", "IB", "SessionID"})
PERSIST_KEYS = frozenset({"Usr"})


# ============================================================
# =  PARSING UTILITIES
# ============================================================

def get_file_line_count(file_path: str) -> int:
    """Быстро подсчитываем количество строк в файле"""
    try:
        with open(file_path, 'rb') as f:
            lines = 0
            buf_size = 1024 * 1024
            read_f = f.raw.read

            buf = read_f(buf_size)
            while buf:
                lines += buf.count(b'\n')
                buf = read_f(buf_size)

            return lines
    except Exception:
        return 0

def parse_event_start(line: str) -> dict | None:
    if m := EVENT_START_RE.match(line):
        return {
            "minute": int(m.group(1)),
            "second": int(m.group(2)),
            "microsecond": int(m.group(3)),
            "duration": int(m.group(4)),
            "event_name": m.group(5)
        }
    return None

def save_replaced_active_op_log(op_key: OpKey, op: dict, replaced_by: dict | None = None) -> None:
    saved_at = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    out_file = replaced_ops_dir / f"replaced-op-db{op_key[0]}-id{op_key[1]}-{saved_at}.log"
    events = op.get("events", [])

    def fmt_ts(value: object) -> str:
        if isinstance(value, datetime):
            return value.isoformat(timespec="microseconds")
        return str(value) if value is not None else ""

    first_event_name = ""
    if events:
        first_event_name = events[0].get("meta", {}).get("event_name", "")

    replacer_event_name = (replaced_by or {}).get("event_name", "")
    replacer_ts = fmt_ts((replaced_by or {}).get("ts"))
    replacer_source = (replaced_by or {}).get("source_file", "")
    replacer_connect = (replaced_by or {}).get("connect_id", "")

    try:
        with open(out_file, "w", encoding="utf-8-sig") as f:
            f.write("# Replaced active operation diagnostic\n")
            f.write(f"# victim_op_key={op_key}\n")
            f.write(f"# victim_start_event={first_event_name}\n")
            f.write(f"# victim_start_ts={fmt_ts(op.get('ts_vrsrequest'))}\n")
            f.write(f"# victim_connect_id={op.get('connect') or ''}\n")
            f.write(f"# replacer_event={replacer_event_name}\n")
            f.write(f"# replacer_ts={replacer_ts}\n")
            f.write(f"# replacer_source_file={replacer_source}\n")
            f.write(f"# replacer_connect_id={replacer_connect}\n")
            f.write("# --- original events ---\n")
            for ev in events:
                text = ev.get("text")
                if not text:
                    continue
                f.write(text)
                if not text.endswith("\n"):
                    f.write("\n")

        logger.warning(
            "Saved interrupted operation %s: %s events -> %s; replaced_by(event=%s, ts=%s, source=%s, connect=%s)",
            op_key,
            len(events),
            out_file,
            replacer_event_name or "unknown",
            replacer_ts or "unknown",
            replacer_source or "unknown",
            replacer_connect or "unknown",
        )
    except Exception as e:
        logger.warning(f"Не удалось сохранить незакрытую операцию {op_key}: {e}")

def _find_prop_value(event: str, key: str) -> str | None:
    token = PROP_KEY_TOKENS.get(key)
    if not token:
        return None

    start = 0
    while True:
        idx = event.find(token, start)
        if idx == -1:
            return None
        # Избегаем ложных срабатываний внутри более длинных идентификаторов.
        if idx > 0:
            prev_ch = event[idx - 1]
            if prev_ch.isalnum() or prev_ch in "_":
                start = idx + 1
                continue

        val_start = idx + len(token)
        val_end = val_start
        event_len = len(event)
        while val_end < event_len and event[val_end] not in ",\r\n":
            val_end += 1
        value = event[val_start:val_end].strip().strip("'\"")
        return value or None


def _extract_context(event: str) -> str | None:
    if "Context=" not in event:
        return None
    if match := CONTEXT_PATTERN.search(event):
        for val in match.groups():
            if val:
                cleaned = val.strip()
                return cleaned or None
    return None


def extract_props(
    event: str,
    needed_keys: set[str] | tuple[str, ...] | None = None,
    include_context: bool = True,
    base_props: dict | None = None,
) -> dict:
    props = dict(base_props) if base_props else {}
    keys = needed_keys if needed_keys is not None else PROP_KEYS

    for key in keys:
        if key in props:
            continue
        value = _find_prop_value(event, key)
        if value is not None:
            props[key] = value

    if include_context and "Context" not in props:
        context_val = _extract_context(event)
        if context_val is not None:
            props["Context"] = context_val

    return props

def resolve_db_name(event_name: str, props: dict) -> str | None:
    if event_name == "SESN":
        return props.get("IB")
    return props.get("p:processName")

def normalize_db_name(db_name: str | None) -> str | None:
    if not db_name:
        return None
    return db_name.strip().casefold()

def normalize_session_id(session_id: str | None) -> str | None:
    if not session_id:
        return None
    if session_id.isdigit():
        return session_id
    match = SESSION_ID_RE.match(session_id)
    return match.group(1) if match else None

def update_op_props(op: dict, props: dict) -> None:
    """Обновляет session в active_ops, если они ещё не заданы."""
    if not op.get("session"):
        session_id = normalize_session_id(props.get("SessionID"))
        if session_id:
            op["session"] = session_id

    if not op.get("connect"):
        connect_id = props.get("t:connectID")        
        if connect_id and connect_id.isdigit():
            op["connect"] = connect_id

    if not op.get("client"):
        client_id = props.get("t:clientID")        
        if client_id and client_id.isdigit():
            op["client"] = client_id

def extract_first_field(events: list, key: str) -> str | None:
    for event in events:
        value = event.get("props", {}).get(key)
        if value:  # фильтруем None и пустые строки
            return value
    return None

_FILENAME_TIME_CACHE: dict[str, tuple[int, int, int, int]] = {}

def _get_time_parts_from_filename(filename: str) -> tuple[int, int, int, int]:
    cached = _FILENAME_TIME_CACHE.get(filename)
    if cached is not None:
        return cached
    # filename уже без пути (используем .name выше); берём без расширения, если оно есть
    dot = filename.rfind('.')
    base = filename[:dot] if dot != -1 else filename
    # YYMMDDHH...
    year = 2000 + int(base[0:2])
    month = int(base[2:4])
    day = int(base[4:6])
    hour = int(base[6:8])
    res = (year, month, day, hour)
    _FILENAME_TIME_CACHE[filename] = res
    return res

def build_ts(filename: str, meta: dict) -> datetime:
    year, month, day, hour = _get_time_parts_from_filename(filename)
    return datetime(year, month, day, hour, meta["minute"], meta["second"], meta["microsecond"])

def to_int_safe(value: str | int | None, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, int):
            return value
        return int(value)
    except (ValueError, TypeError):
        return default

def extract_metric_value(event_text: str, key: str) -> int:
    """Извлекает числовое значение метрики вида Key=123 из строки события."""
    match = re.search(rf"\b{re.escape(key)}=([^,\r\n]+)", event_text)
    return to_int_safe(match.group(1) if match else None)


def encode_json_row(row: dict) -> bytes:
    if orjson is not None:
        return orjson.dumps(row)
    return json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# ============================================================
# =  CLICKHOUSE UTILITIES
# ============================================================

def insert_batch(table: str, rows: list[dict]) -> None:
    if not rows:
        return

    rows_count = len(rows)
    body_sql = (
        f"INSERT INTO {CLICKHOUSE_DATABASE}.{table} FORMAT JSONEachRow\n".encode("utf-8")
        + b"\n".join(encode_json_row(r) for r in rows)
    )

    request_data = body_sql
    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "Accept": "text/plain",
    }
    if INSERT_COMPRESSION == "gzip":
        request_data = gzip.compress(body_sql, compresslevel=GZIP_LEVEL)
        headers["Content-Encoding"] = "gzip"

    ok = False
    try:
        resp = SESSION.post(
            f"{CLICKHOUSE_URL}/",
            params={
                "user": CLICKHOUSE_USER, 
                "password": CLICKHOUSE_PASSWORD, 
                "database": CLICKHOUSE_DATABASE,
                "input_format_parallel_parsing": 0,
                "enable_http_compression": 1
            },
            data=request_data,
            headers=headers,
            timeout=(5, TIMEOUT),
            proxies={}
        )
        ok = resp.status_code == 200
        if resp.status_code != 200:
            print(f"[ERROR] Insert into {table} failed: HTTP {resp.status_code}. Body: {resp.text[:800]} (rows={rows_count})")
    except requests.RequestException as e:
        print(f"[ERROR] Insert failed into {table}: {e} (rows={rows_count})")

# <<< быстрая очистка текущего набора данных
def clear_dataset(dataset: str) -> None:
    for table in (TABLE_EVENTS, TABLE_OPERATIONS, TABLE_EVENT_STATS, TABLE_CALL_OPS):
        q = f"ALTER TABLE {CLICKHOUSE_DATABASE}.{table} DELETE WHERE dataset = '{dataset}'"
        try:
            resp = SESSION.post(
                f"{CLICKHOUSE_URL}/",
                params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD, "database": CLICKHOUSE_DATABASE, "query": q},
                timeout=TIMEOUT,
                proxies={}
            )
            if resp.status_code == 200:
                print(f"[INFO] Очищен набор данных '{dataset}' в таблице {table}")
            else:
                # Если таблицы нет (например, context_tree_rows) — просто сообщим
                print(f"[WARN] Clear dataset in {table}: HTTP {resp.status_code}, {resp.text[:200]}")
        except requests.RequestException as e:
            print(f"[WARN] Clear dataset in {table} failed: {e}")


# ============================================================
# =  OPERATIONS & EVENT STATS
# ============================================================

def calculate_event_stats(op: dict) -> dict:
    """
    Вычисляет статистику длительности событий по именам для операции.
    Исключает VRSREQUEST и VRSRESPONSE из статистики.
    Возвращает словарь с суммарной длительностью, процентом и количеством для каждого типа события.
    """
    event_stats = {}  # {event_name: {"total_duration_us": int, "count": int}}
    
    for event_data in op["events"]:
        meta = event_data["meta"]
        event_name = meta["event_name"]
        
        # Исключаем VRSREQUEST и VRSRESPONSE из статистики
        if event_name in ("VRSREQUEST", "VRSRESPONSE", "SESN"):
            continue
            
        duration_us = int(meta["duration"])
        
        if event_name in event_stats:
            event_stats[event_name]["total_duration_us"] += duration_us
            event_stats[event_name]["count"] += 1
        else:
            event_stats[event_name] = {
                "total_duration_us": duration_us,
                "count": 1
            }
    
    # Вычисляем общую длительность всех событий
    op_duration_us = to_int_safe(op.get("duration_us"), 0)
    
    for event_name, stats in event_stats.items():
        duration_us = stats["total_duration_us"]
        percentage = round(duration_us * 100.0 / op_duration_us, 2) if op_duration_us > 0 else 0.0
        stats["percentage"] = percentage
    
    return event_stats


# ============================================================
# =  MULTI-CATALOG HANDLING
# ============================================================

def discover_rphost_groups(input_dir: str) -> dict[str, list[str]]:
    """
    Рекурсивно ищет каталоги rphost_* в input_dir.
    Если input_dir уже является каталогом rphost_*, возвращает только его.
    """
    groups = defaultdict(list)
    root = Path(input_dir).resolve()

    # --- 1. Если сам input_dir уже rphost_* ---
    if root.name.startswith("rphost_") and root.is_dir():
        groups[root.name].append(str(root))
        return dict(groups)

    # --- 2. Иначе ищем рекурсивно внутри ---
    for rphost_dir in root.rglob("rphost_*"):  # рекурсивный обход на любой глубине
        if rphost_dir.is_dir():
            groups[rphost_dir.name].append(str(rphost_dir))

    return dict(groups)

def discover_rmngr_groups(input_dir: str) -> dict[str, list[str]]:
    """
    Рекурсивно ищет каталоги rmngr_* в input_dir.
    Если input_dir уже является каталогом rmngr_*, возвращает только его.
    """
    groups = defaultdict(list)
    root = Path(input_dir).resolve()

    if root.name.startswith("rmngr_") and root.is_dir():
        groups[root.name].append(str(root))
        return dict(groups)

    for rmngr_dir in root.rglob("rmngr_*"):
        if rmngr_dir.is_dir():
            groups[rmngr_dir.name].append(str(rmngr_dir))

    return dict(groups)

def group_files_by_hour(files: list[str]) -> dict[str, list[str]]:
    """
    Группирует список файлов по часу.
    Ключ часа – первые 8 символов имени файла: YYMMDDHH (как в build_ts).
    """
    grouped = defaultdict(list)
    for fp in files:
        base = Path(fp).stem
        if len(base) >= 8:
            hour_key = base[:8]
            grouped[hour_key].append(fp)
    return grouped

def is_rmngr_path(path: str) -> bool:
    """Возвращает True, если путь относится к каталогу rmngr_*."""
    return any(part.startswith("rmngr_") for part in Path(path).parts)

def get_process_id(path: str) -> str:
    """Возвращает идентификатор процесса (rphost_*/rmngr_*) для изоляции ключей операций."""
    p = Path(path)
    for part in reversed(p.parts):
        if part.startswith("rphost_") or part.startswith("rmngr_"):
            return part
    return p.parent.name or p.name

def iter_events(file_path: str):
    """
    Итератор событий из одного файла.
    Возвращает кортеж (ts: datetime, event_meta: dict, event_text: str), где event_text – событие целиком.
    """
    filename = Path(file_path).name
    buffer: list[str] = []
    event_meta: dict | None = None

    try:
        with open(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
            for line in f:
                ev_start = parse_event_start(line)
                if ev_start:
                    # закончим предыдущее событие
                    if buffer and event_meta:
                        ts = build_ts(filename, event_meta)
                        yield ts, event_meta, "".join(buffer)
                        buffer.clear()
                    # начнем новое
                    event_meta = ev_start
                    buffer.append(line)
                else:
                    buffer.append(line)

            # хвост
            if buffer and event_meta:
                ts = build_ts(filename, event_meta)
                yield ts, event_meta, "".join(buffer)

    except Exception as e:
        print(f"[WARN] Ошибка чтения {file_path}: {e}")

def merge_hour_events(hour_files: list[str], out_file: Path) -> int:
    """
    Объединяет события из списка файлов за один час в out_file по временной метке.
    Имя out_file должно быть вида <YYMMDDHH>.log (без суффиксов).
    Возвращает количество записанных событий.
    """
    # heap элементов: (ts, idx, meta, text, gen, is_rmngr)
    # idx нужен для устойчивости сравнения при равных ts
    heap: list[tuple[datetime, int, dict, str, object, bool]] = []
    idx_counter = 0

    def next_allowed(gen, is_rmngr: bool):
        """Возвращает следующее нужное событие, пропуская нерелевантные для rmngr."""
        while True:
            ts_val, meta_val, text_val = next(gen)
            if is_rmngr and meta_val.get("event_name") != "SESN":
                continue
            return ts_val, meta_val, text_val

    # подготовим генераторы и закинем по первому событию в кучу
    gens = []
    for fp in hour_files:
        gen = iter_events(fp)
        try:
            is_rmngr = is_rmngr_path(fp)
            ts, event_meta, text = next_allowed(gen, is_rmngr)
            heapq.heappush(heap, (ts, idx_counter, event_meta, text, gen, is_rmngr))
            idx_counter += 1
            gens.append(gen)
        except StopIteration:
            continue

    out_file.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(out_file, "w", encoding="utf-8-sig") as out:
        while heap:
            ts, _, _, text, gen, is_rmngr = heapq.heappop(heap)
            out.write(text)
            count += 1
            try:
                ts2, meta2, text2 = next_allowed(gen, is_rmngr)
                heapq.heappush(heap, (ts2, idx_counter, meta2, text2, gen, is_rmngr))
                idx_counter += 1
            except StopIteration:
                pass

    return count

def iter_read_events(hour_files: list[str]):
    """
    Потоково отдает события из файлов одного часа.
    Возвращает кортеж (ts, event_meta, event_text, source_path), при этом в памяти
    держится только по одному событию от каждого файла.
    """
    heap: list[tuple[datetime, int, int, dict, str, object, str, bool]] = []
    order_counter = 0

    def next_allowed(gen, is_rmngr: bool):
        """Возвращает следующее нужное событие, пропуская нерелевантные для rmngr."""
        while True:
            ts_val, meta_val, text_val = next(gen)
            if is_rmngr and meta_val.get("event_name") != "SESN":
                continue
            return ts_val, meta_val, text_val

    for idx, fp in enumerate(sorted(hour_files)):
        gen = iter_events(fp)
        try:
            is_rmngr = is_rmngr_path(fp)
            ts, event_meta, text = next_allowed(gen, is_rmngr)
            heapq.heappush(heap, (ts, idx, order_counter, event_meta, text, gen, fp, is_rmngr))
            order_counter += 1
        except StopIteration:
            continue

    while heap:
        ts, idx, _, event_meta, text, gen, fp, is_rmngr = heapq.heappop(heap)
        yield ts, event_meta, text, fp
        try:
            ts_next, meta_next, text_next = next_allowed(gen, is_rmngr)
            heapq.heappush(heap, (ts_next, idx, order_counter, meta_next, text_next, gen, fp, is_rmngr))
            order_counter += 1
        except StopIteration:
            pass

def build_temp_for_multi(input_dir: str, temp_dir: str) -> None:
    temp_root = Path(temp_dir)
    if temp_root.exists():
        shutil.rmtree(temp_root)
    temp_root.mkdir(parents=True, exist_ok=True)

    rphost_groups = discover_rphost_groups(input_dir)
    rmngr_groups = discover_rmngr_groups(input_dir) if PROCESS_BACKGROUND_JOBS else {}

    all_groups = {}
    all_groups.update(rphost_groups)
    all_groups.update(rmngr_groups)

    sorted_groups = sorted(all_groups.items())
    total_processes = len(sorted_groups)
    print(f"[INFO] Найдено процессов: {total_processes} - {', '.join(name for name, _ in sorted_groups)}")

    for proc_idx, (proc_name, proc_dirs) in enumerate(sorted_groups, start=1):
        # соберем все .log этого процесса из всех типовых каталогов
        all_logs: list[str] = []
        for d in proc_dirs:
            all_logs.extend(sorted(glob.glob(str(Path(d) / "*.log"))))

        if not all_logs:
            print(f"[WARN] Процесс {proc_idx}/{total_processes} {proc_name}: каталогов {len(proc_dirs)}, файлов 0 - пропускаю")
            continue
        total_logs = len(all_logs)
        print(f"[INFO] Процесс {proc_idx}/{total_processes} {proc_name}: каталогов {len(proc_dirs)}, файлов {total_logs}")

        # сгруппируем по часу
        by_hour = group_files_by_hour(all_logs)
        out_proc_dir = temp_root / proc_name
        out_proc_dir.mkdir(parents=True, exist_ok=True)
        total_hours = len(by_hour)
        processed_logs = 0

        for hour_idx, (hour_key, hour_files) in enumerate(sorted(by_hour.items()), start=1):
            out_file = out_proc_dir / f"{hour_key}.log"   # без суффиксов
            cnt = merge_hour_events(hour_files, out_file)
            processed_logs += len(hour_files)
            print(
                f"[INFO] {proc_name}/{out_file.name}: "
                f"объединено {cnt:,} событий из {len(hour_files)} файлов "
                f"(обработано файлов {processed_logs}/{total_logs})"
            )

    print(f"[INFO] Временная структура собрана: {temp_root}")


# ============================================================
# =  MAIN LOG PARSER
# ============================================================

def process_logs(input_dir: str) -> None:
    # Читаем и rphost_*, и rmngr_* в режиме single
    rphost_files = glob.glob(str(Path(input_dir) / "**" / "rphost_*" / "*.log"), recursive=True)
    rmngr_files = glob.glob(str(Path(input_dir) / "**" / "rmngr_*" / "*.log"), recursive=True) if PROCESS_BACKGROUND_JOBS else []
    files = sorted(set(rphost_files + rmngr_files))

    logger.info(f"Найдено файлов rphost: {len(rphost_files)}, rmngr: {len(rmngr_files)}")
    print(f"[INFO] Найдено файлов rphost: {len(rphost_files)}, rmngr: {len(rmngr_files)}")

    if not files:
        logger.warning("Нет входных файлов rphost_/rmngr_* для обработки")
        print("[WARN] Нет входных файлов rphost_/rmngr_* для обработки")
        return

    by_hour = group_files_by_hour(files)
    print(f"[INFO] Часовых групп: {len(by_hour)}")
    logger.info(f"Часовых групп: {len(by_hour)}")

    active_ops: Dict[OpKey, Dict] = {}
    pending_call: Dict[OpKey, Dict] = {}
    early_call: Dict[OpKey, Dict] = {}
    process_id_cache: dict[str, str] = {}
    batch_ops, batch_events, batch_event_stats, batch_calls = [], [], [], []
    total_operations = 0
    db_ids: Dict[str, int] = {}
    next_db_id = 1

    def flush_batches() -> tuple[int, int, int, int]:
        counts = (len(batch_events), len(batch_ops), len(batch_event_stats), len(batch_calls))
        if not any(counts):
            return counts


        insert_batch(TABLE_OPERATIONS, batch_ops)
        insert_batch(TABLE_EVENTS, batch_events)
        insert_batch(TABLE_EVENT_STATS, batch_event_stats)
        insert_batch(TABLE_CALL_OPS, batch_calls)

        batch_events.clear()
        batch_ops.clear()
        batch_event_stats.clear()
        batch_calls.clear()
        return counts

    def get_db_id(db_name: str | None) -> int:
        nonlocal next_db_id
        if not db_name:
            return 0
        db_id = db_ids.get(db_name)
        if db_id is None:
            db_id = next_db_id
            db_ids[db_name] = db_id
            next_db_id += 1
        return db_id

    def build_op_key(db_name: str | None, op_id: str | None, process_id: str, op_kind: str) -> OpKey | None:
        normalized_id = normalize_session_id(op_id)
        if not normalized_id:
            return None
        process_part = "" if op_kind == "sesn" else process_id
        key = (get_db_id(normalize_db_name(db_name)), int(normalized_id), process_part, op_kind)
        return key

    def handle_event(event_meta: dict, event_text: str, ts_event: datetime, source_path: str):
        """Обрабатывает одно событие, обновляя активные операции и пачки."""
        event_name = event_meta["event_name"]
        if event_name in ("VRSREQUEST", "VRSRESPONSE") and not PROCESS_CLIENT_SERVER_OPS:
            return
        if event_name == "SESN" and not PROCESS_BACKGROUND_JOBS:
            return

        def append_call_rows(pending: dict, call_meta: dict, call_text: str, call_props: dict, call_ts: datetime) -> None:
            """Добавляет CALL в batch_calls и batch_events, используя границы операции из pending_call."""
            session_id_val = to_int_safe(normalize_session_id(call_props.get("SessionID")))
            if session_id_val == 0:
                session_id_val = to_int_safe(pending.get("session_id"))
            client_id_val = to_int_safe(pending.get("client_id"))

            batch_calls.append({
                "dataset": DATASET_NAME,
                "db_name": pending.get("db_name") or "",
                "event_name": call_meta.get("event_name") or "CALL",
                "duration_us": to_int_safe(call_meta.get("duration")),
                "session_id": session_id_val,
                "client_id": client_id_val,
                "cpu_time": extract_metric_value(call_text, "CpuTime"),
                "memory": extract_metric_value(call_text, "Memory"),
                "memory_peak": extract_metric_value(call_text, "MemoryPeak"),
                "connect_id": to_int_safe(pending.get("connect_id")),
                "ts_vrsrequest_us": pending.get("ts_vrsrequest_us"),
                "ts_vrsresponse_us": pending.get("ts_vrsresponse_us"),
            })
            batch_events.append({
                "dataset": DATASET_NAME,
                "db_name": pending.get("db_name") or "",
                "session_id": session_id_val,
                "connect_id": to_int_safe(pending.get("connect_id")),
                "client_id": client_id_val,
                "user": call_props.get("Usr"),
                "ts_vrsrequest_us": pending.get("ts_vrsrequest_us"),
                "ts_vrsresponse_us": pending.get("ts_vrsresponse_us"),
                "ts_event_us": call_ts.isoformat(timespec="microseconds"),
                "ts_event": call_ts.replace(microsecond=0).isoformat(),
                "event_name": call_meta.get("event_name") or "CALL",
                "duration_us": to_int_safe(call_meta.get("duration")),
                "space_us": 0,
                "event_string": call_text,
                "context": call_props.get("Context"),
            })

        def get_valid_pending_for_call(client_key: OpKey | None, sesn_key: OpKey | None, call_ts: datetime) -> tuple[OpKey | None, dict | None]:
            for candidate_key in (client_key, sesn_key):
                if not candidate_key:
                    continue
                pending = pending_call.get(candidate_key)
                if not pending:
                    continue

                response_ts = pending.get("ts_vrsresponse_dt")
                if not isinstance(response_ts, datetime):
                    try:
                        response_ts = datetime.fromisoformat(str(pending.get("ts_vrsresponse_us")))
                    except Exception:
                        response_ts = None

                if not isinstance(response_ts, datetime):
                    pending_call.pop(candidate_key, None)
                    continue

                gap_sec = abs((call_ts - response_ts).total_seconds())
                if gap_sec > CALL_BIND_TTL_SECONDS:
                    pending_call.pop(candidate_key, None)
                    logger.warning(
                        "Dropped stale pending CALL bind: key=%s, gap=%.3fs, ttl=%ss",
                        candidate_key,
                        gap_sec,
                        CALL_BIND_TTL_SECONDS,
                    )
                    continue

                if gap_sec > CALL_EXPECTED_WINDOW_SECONDS:
                    logger.warning(
                        "CALL bind outside expected window: key=%s, gap=%.3fs, expected=%ss",
                        candidate_key,
                        gap_sec,
                        CALL_EXPECTED_WINDOW_SECONDS,
                    )
                return candidate_key, pending
            return None, None

        def complete_operation(op_key: OpKey) -> None:
            """
            Добавляет текущее событие в операцию, ставит ts_vrsresponse, считает длительность,
            применяет фильтры, пушит батчи и убирает запись из active_ops.
            """
            nonlocal total_operations  # нужен для инкремента счётчика операций
            op = active_ops[op_key]
            update_op_props(op, props)
            op["events"].append({"meta": event_meta, "props": props, "text": event_text, "ts": ts_event})
            op["ts_vrsresponse"] = ts_event

            delta = op["ts_vrsresponse"] - op["ts_vrsrequest"]
            op["duration_us"] = delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds

            duration_us = op.get("duration_us", 0)
            events_count = len(op["events"])
            min_duration_us = MIN_OPERATION_DURATION_MS * 1000
            if duration_us < min_duration_us or events_count < MIN_OPERATION_EVENTS:
                early_call.pop(op_key, None)
                del active_ops[op_key]
                return

            early_item = early_call.pop(op_key, None)

            if op.get("session"):
                total_operations += 1

                batch_ops.append({
                    "dataset": DATASET_NAME,
                    "db_name": op.get("db_name") or "",
                    "session_id": to_int_safe(op.get("session")),
                    "connect_id": to_int_safe(op.get("connect")),
                    "client_id": to_int_safe(op.get("client")),
                    "user": extract_first_field(op["events"], "Usr"),
                    "ts_vrsrequest_us": op["ts_vrsrequest"].isoformat(timespec="microseconds"),
                    "ts_vrsresponse_us": op["ts_vrsresponse"].isoformat(timespec="microseconds"),
                    "ts_vrsrequest": op["ts_vrsrequest"].replace(microsecond=0).isoformat(),
                    "ts_vrsresponse": op["ts_vrsresponse"].replace(microsecond=0).isoformat(),
                    "duration_us": to_int_safe(op.get("duration_us", 0)),
                    "context": extract_first_field(op["events"], "Context"),
                })

                if event_name in ("VRSRESPONSE", "SESN") and op_key:
                    pending_call[op_key] = {
                        "db_name": op.get("db_name") or "",
                        "session_id": to_int_safe(op.get("session")),
                        "connect_id": to_int_safe(op.get("connect")),
                        "client_id": to_int_safe(op.get("client")) or op_key[1],
                        "ts_vrsrequest_us": op["ts_vrsrequest"].isoformat(timespec="microseconds"),
                        "ts_vrsresponse_us": op["ts_vrsresponse"].isoformat(timespec="microseconds"),
                        "ts_vrsresponse_dt": op["ts_vrsresponse"],
                    }
                    if early_item:
                        pending = pending_call[op_key]
                        early_ts = early_item.get("ts")
                        if isinstance(early_ts, datetime):
                            gap_sec = abs((early_ts - op["ts_vrsresponse"]).total_seconds())
                            if gap_sec <= CALL_BIND_TTL_SECONDS:
                                if gap_sec > CALL_EXPECTED_WINDOW_SECONDS:
                                    logger.warning(
                                        "EARLY CALL bind outside expected window: key=%s, gap=%.3fs, expected=%ss",
                                        op_key,
                                        gap_sec,
                                        CALL_EXPECTED_WINDOW_SECONDS,
                                    )
                                append_call_rows(
                                    pending,
                                    early_item["meta"],
                                    early_item["text"],
                                    early_item["props"],
                                    early_ts,
                                )
                            else:
                                logger.warning(
                                    "Dropped stale EARLY CALL bind: key=%s, gap=%.3fs, ttl=%ss",
                                    op_key,
                                    gap_sec,
                                    CALL_BIND_TTL_SECONDS,
                                )
                        pending_call.pop(op_key, None)

                prev_ts = None
                for ev_data in op["events"]:
                    ts = ev_data["ts"]
                    meta = ev_data["meta"]
                    props_inner = ev_data["props"]
                    event_string = ev_data["text"]
                    space = int((ts - prev_ts).total_seconds() * 1_000_000 - int(meta["duration"])) if prev_ts else 0
                    prev_ts = ts

                    row = {
                        "dataset": DATASET_NAME,
                        "db_name": op.get("db_name") or "",
                        "session_id": to_int_safe(op.get("session")),
                        "connect_id": to_int_safe(op.get("connect")),
                        "client_id": to_int_safe(op.get("client")),
                        "user": props_inner.get("Usr"),
                        "ts_vrsrequest_us": op["ts_vrsrequest"].isoformat(timespec="microseconds"),
                        "ts_vrsresponse_us": op["ts_vrsresponse"].isoformat(timespec="microseconds"),
                        "ts_event_us": ts.isoformat(timespec="microseconds"),
                        "ts_event": ts.replace(microsecond=0).isoformat(),
                        "event_name": meta["event_name"],
                        "duration_us": int(meta["duration"]),
                        "space_us": space,
                        "event_string": event_string,
                        "context": props_inner.get("Context"),
                    }
                    batch_events.append(row)

                event_stats = calculate_event_stats(op)
                for stats_event_name, stats in event_stats.items():
                    batch_event_stats.append({
                        "dataset": DATASET_NAME,
                        "db_name": op.get("db_name") or "",
                        "session_id": to_int_safe(op.get("session")),
                        "connect_id": to_int_safe(op.get("connect")),
                        "client_id": to_int_safe(op.get("client")),
                        "ts_vrsrequest_us": op["ts_vrsrequest"].isoformat(timespec="microseconds"),
                        "ts_vrsresponse_us": op["ts_vrsresponse"].isoformat(timespec="microseconds"),
                        "event_name": stats_event_name,
                        "total_duration_us": stats["total_duration_us"],
                        "count": stats["count"],
                        "percentage": stats["percentage"],
                    })
            del active_ops[op_key]

        def start_operation(op_key: OpKey, session_value: str | None, connect_value: str | None, db_name_value: str | None) -> None:
            """
            Инициализирует новую операцию (VRSREQUEST/SESN Start): кладёт первую точку и обновляет props.
            extra_fields позволяет добавить специфичные поля без дублирования кода.
            """
            if op_key in active_ops:
                save_replaced_active_op_log(
                    op_key,
                    active_ops[op_key],
                    replaced_by={
                        "event_name": event_name,
                        "ts": ts_event,
                        "source_file": source_path,
                        "connect_id": connect_id,
                    },
                )
                early_call.pop(op_key, None)
                active_ops.pop(op_key, None)
            active_ops[op_key] = {
                "db_name": db_name_value or "",
                "session": session_value,
                "connect": connect_value,
                "ts_vrsrequest": ts_event,
                "events": [{"meta": event_meta, "props": props, "text": event_text, "ts": ts_event}],
            }
            update_op_props(active_ops[op_key], props)

        props: dict = {}

        def ensure_props(keys: set[str] | tuple[str, ...] | frozenset[str], include_context: bool = False) -> None:
            nonlocal props
            props = extract_props(
                event_text,
                needed_keys=keys,
                include_context=include_context,
                base_props=props,
            )

        route_keys = ROUTE_KEYS_SESN if event_name == "SESN" else ROUTE_KEYS_DEFAULT
        ensure_props(route_keys, include_context=False)

        client_id = props.get("t:clientID")
        session_id = normalize_session_id(props.get("SessionID"))
        connect_id = props.get("t:connectID")
        db_name = normalize_db_name(resolve_db_name(event_name, props))
        process_id = process_id_cache.get(source_path)
        if process_id is None:
            process_id = get_process_id(source_path)
            process_id_cache[source_path] = process_id

        if event_name == "VRSREQUEST":
            client_key = build_op_key(db_name, client_id, process_id, "vrs")
            pending_call.pop(client_key, None)
            if client_key:
                start_operation(client_key, session_id, connect_id, db_name)

        elif event_name == "VRSRESPONSE":
            client_key = build_op_key(db_name, client_id, process_id, "vrs")
            if client_key in active_ops:
                complete_operation(client_key)

        elif event_name == "CALL":
            min_duration_us = MIN_OPERATION_DURATION_MS * 1000
            if to_int_safe(event_meta.get("duration")) < min_duration_us:
                return
            client_key = build_op_key(db_name, client_id, process_id, "vrs")
            sesn_key = build_op_key(db_name, session_id, process_id, "sesn")
            pending_key, pending = get_valid_pending_for_call(client_key, sesn_key, ts_event)
            if pending:
                ensure_props(PERSIST_KEYS, include_context=True)
                append_call_rows(pending, event_meta, event_text, props, ts_event)
                if pending_key:
                    pending_call.pop(pending_key, None)
            else:
                active_key: OpKey | None = None
                if client_key in active_ops:
                    active_key = client_key
                elif sesn_key in active_ops:
                    active_key = sesn_key
                if active_key:
                    if active_key not in early_call:
                        ensure_props(PERSIST_KEYS, include_context=True)
                        early_call[active_key] = {
                            "meta": event_meta,
                            "props": props,
                            "text": event_text,
                            "ts": ts_event,
                        }

        elif event_name == "SESN":
           
            func = props.get("Func")
            appl = props.get("Appl")
            session_id = props.get("Nmb")
            session_key = build_op_key(db_name, session_id, process_id, "sesn")

            if func in ("Start", "Restore") and appl == "BackgroundJob" and session_key:
                pending_call.pop(session_key, None)
                start_operation(session_key, session_id, connect_id, db_name)

            elif func == "Finish" and session_key in active_ops: 
                complete_operation(session_key)

        else:
            client_key = build_op_key(db_name, client_id, process_id, "vrs")
            session_key = build_op_key(db_name, session_id, process_id, "sesn")
            if client_key in active_ops:
                op = active_ops[client_key]
            elif session_key in active_ops:
                op = active_ops[session_key]
            else:
                return            
            ensure_props(PERSIST_KEYS, include_context=True)
            update_op_props(op, props)
            op["events"].append({"meta": event_meta, "props": props, "text": event_text, "ts": ts_event})

        if len(batch_events) >= BATCH_SIZE:
            flush_batches()

    total_hours = len(by_hour)
    for hour_idx, (hour_key, hour_files) in enumerate(sorted(by_hour.items()), start=1):
        print(f"\n[INFO] Час {hour_key}: файлов {len(hour_files)} (rphost+rmngr)")
        logger.info(f"Начало обработки часа {hour_key}: {len(hour_files)} файлов")

        # Прогресс-бар по строкам всех файлов часа
        total_lines_hour = None
        if COUNT_LINES_FOR_PROGRESS:
            total_lines_hour = sum(get_file_line_count(fp) for fp in hour_files)
        with tqdm(
            total=total_lines_hour if (total_lines_hour is not None and total_lines_hour > 0) else None,
            desc=f"Час {hour_idx}/{total_hours}",
            unit="строк",
            unit_scale=True
        ) as pbar:
            lines_since_update = 0
            reader = iter_read_events(hour_files)
            while True:
                try:
                    ts_event, event_meta, event_text, source_path = next(reader)
                except StopIteration:
                    break

                if not event_meta:
                    continue

                handle_event(event_meta, event_text, ts_event, source_path)

                # обновляем прогресс каждые 10k прочитанных строк
                lines_in_event = max(1, event_text.count("\n"))
                lines_since_update += lines_in_event
                if lines_since_update >= 10_000:
                    pbar.update(lines_since_update)
                    pbar.set_postfix({"Операции": total_operations})
                    lines_since_update = 0


            if lines_since_update:
                pbar.update(lines_since_update)
                pbar.set_postfix({"Операции": total_operations})

    final_batch_events, final_batch_ops, final_batch_stats, final_batch_calls = flush_batches()
    if any((final_batch_events, final_batch_ops, final_batch_stats, final_batch_calls)):
        logger.info(f"Отправлена финальная пачка: {final_batch_events} событий, {final_batch_ops} операций")


    completion_msg = f"Готово: обработано {total_operations} операций"
    logger.info(completion_msg)
    print(f"[INFO] {completion_msg}")
    
def main() -> None:
    logger.info(f"Запуск скрипта trace-vrs. Лог сохраняется в: {log_file}")
    print(f"[INFO] Лог сохраняется в: {log_file}")
    logger.info(
        "Настройки: count_lines_for_progress=%s, insert_compression=%s, gzip_level=%s, json_serializer=%s",
        COUNT_LINES_FOR_PROGRESS,
        INSERT_COMPRESSION,
        GZIP_LEVEL,
        JSON_SERIALIZER_NAME,
    )
    print(
        f"[INFO] Настройки: count_lines_for_progress={COUNT_LINES_FOR_PROGRESS}, "
        f"insert_compression={INSERT_COMPRESSION}, gzip_level={GZIP_LEVEL}, "
        f"json_serializer={JSON_SERIALIZER_NAME}"
    )

    # очистим текущий dataset перед загрузкой
    clear_dataset(DATASET_NAME)

    INPUT_DIR = config["paths"]["input_dir"]
    TEMP_DIR = config["paths"].get("temp_dir", str(Path(INPUT_DIR).parent / "temp"))
    MODE = config["processing"].get("mode", "single")
    CLEANUP_TEMP_DIR = config["processing"].get("cleanup_temp_dir", True)

    logger.info(f"Режим обработки: {MODE}")
    print(f"[INFO] Режим обработки: {MODE}")
    logger.info(f"Каталог логов: {INPUT_DIR}")
    print(f"[INFO] Каталог логов: {INPUT_DIR}")

    try:
        if MODE == "multi":
            # собрать временную структуру rphost_*/YYMMDDHH.log
            build_temp_for_multi(INPUT_DIR, TEMP_DIR)
            # запустить парсер single по temp_dir
            logger.info(f"Старт обработки temp каталога: {TEMP_DIR}")
            print(f"[INFO] Старт обработки temp каталога: {TEMP_DIR}")
            process_logs(TEMP_DIR)
            # cleanup temp dir if requested
            if CLEANUP_TEMP_DIR:
                try:
                    shutil.rmtree(TEMP_DIR, ignore_errors=True)
                    logger.info(f"Temp удален: {TEMP_DIR}")
                    print(f"[INFO] Temp удален: {TEMP_DIR}")
                except Exception as e:
                    logger.warning(f"Не удалось удалить temp: {e}")
                    print(f"[WARN] Не удалось удалить temp: {e}")
            else:
                logger.info(f"Temp сохранен: {TEMP_DIR}")
                print(f"[INFO] Temp сохранен: {TEMP_DIR}")
        else:
            # классический режим
            process_logs(INPUT_DIR)

        logger.info("Скрипт успешно завершен")
        print("[INFO] Скрипт успешно завершен")

    except Exception as e:
        logger.error(f"Ошибка выполнения скрипта: {e}", exc_info=True)
        print(f"[ERROR] Ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
