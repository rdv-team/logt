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
from typing import Dict, List
from datetime import datetime
from tqdm import tqdm

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

# --- Ограничения фильтрации операций ---
MIN_OPERATION_DURATION_MS = config["filtering"]["min_operation_duration_ms"]
MIN_OPERATION_EVENTS = config["filtering"]["min_operation_events"]


# Reuse HTTP connection
SESSION = requests.Session()

# --- Таблицы ClickHouse ---
TABLE_OPERATIONS = "operations"
TABLE_EVENTS = "events"
TABLE_EVENT_STATS = "event_stats"
TABLE_CALL_OPS = "calls"

# --- Компиляция регулярных выражений ---
EVENT_START_RE = re.compile(r"^(\d{2}):(\d{2})\.(\d{6})-(\d+),([A-Za-z]+),")
PROPS_PATTERN = re.compile(r"\b(t:clientID|SessionID|Usr|t:connectID|Appl|Func|Nmb)=([^,\r\n]+)")
CONTEXT_PATTERN = re.compile(r',Context=(?:"([^"]*)"|\'([^\']*)\'|([^,\r\n]+))', re.DOTALL)


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

def extract_props(event: str) -> dict:
    props = {}
    
    # Первый паттерн
    for match in PROPS_PATTERN.finditer(event):
        key, val = match.groups()
        props[key] = val.strip().strip("'\"")
    
    # Второй паттерн - Context (аналогично оригинальной extract_context)
    if match := CONTEXT_PATTERN.search(event):
        for val in match.groups():
            if val:
                props['Context'] = val.strip()
                break 

    return props

def update_op_props(op: dict, props: dict) -> None:
    """Обновляет session в active_ops, если они ещё не заданы."""
    if not op.get("session"):
        session_id = props.get("SessionID")
        if session_id and session_id.isdigit():
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


# ============================================================
# =  CLICKHOUSE UTILITIES
# ============================================================

def insert_batch(table: str, rows: list[dict]) -> None:
    if not rows:
        return

    # <<< добавляем dataset в каждую строку перед сериализацией
    for r in rows:
        r["dataset"] = DATASET_NAME

    body_sql = "INSERT INTO {} FORMAT JSONEachRow\n{}".format(
        f"{CLICKHOUSE_DATABASE}.{table}",   # <<< пишем с префиксом БД
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    ).encode("utf-8")
    gz = gzip.compress(body_sql)

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
            data=gz,
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "Content-Encoding": "gzip",
                "Accept": "text/plain",
                # keep-alive по умолчанию
            },
            timeout=(5, TIMEOUT),
            proxies={}
        )
        if resp.status_code != 200:
            print(f"[ERROR] Insert into {table} failed: HTTP {resp.status_code}. Body: {resp.text[:800]} (rows={len(rows)})")
    except requests.RequestException as e:
        print(f"[ERROR] Insert failed into {table}: {e} (rows={len(rows)})")

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

    print(f"[INFO] Найдено процессов: {len(all_groups)} - {', '.join(sorted(all_groups.keys()))}")

    for proc_name, proc_dirs in sorted(all_groups.items()):
        print(f"[INFO] Процесс {proc_name}: каталогов {len(proc_dirs)}")
        # соберем все .log этого процесса из всех типовых каталогов
        all_logs: list[str] = []
        for d in proc_dirs:
            all_logs.extend(sorted(glob.glob(str(Path(d) / "*.log"))))

        if not all_logs:
            print(f"[WARN] У процесса {proc_name} нет логов - пропускаю")
            continue

        # сгруппируем по часу
        by_hour = group_files_by_hour(all_logs)
        out_proc_dir = temp_root / proc_name
        out_proc_dir.mkdir(parents=True, exist_ok=True)

        for hour_key, hour_files in sorted(by_hour.items()):
            out_file = out_proc_dir / f"{hour_key}.log"   # без суффиксов
            cnt = merge_hour_events(hour_files, out_file)
            print(f"[INFO]   {proc_name}/{out_file.name}: объединено {cnt:,} событий из {len(hour_files)} файлов")

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

    active_ops: Dict[str, Dict] = {}
    pending_ops: Dict[str, Dict] = {}
    batch_ops, batch_events, batch_event_stats, batch_calls = [], [], [], []
    total_operations = 0

    def handle_event(event_meta: dict, event_text: str, ts_event: datetime):
        """Обрабатывает одно событие, обновляя активные операции и пачки."""
        event_name = event_meta["event_name"]
        if event_name in ("VRSREQUEST", "VRSRESPONSE") and not PROCESS_CLIENT_SERVER_OPS:
            return
        if event_name == "SESN" and not PROCESS_BACKGROUND_JOBS:
            return

        def complete_operation(op_key: str) -> None:
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
                del active_ops[op_key]
                return

            if op.get("session"):
                total_operations += 1

                batch_ops.append({
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

                if event_name == "VRSRESPONSE" and op_key:
                    pending_ops[op_key] = {
                        "session_id": to_int_safe(op.get("session")),
                        "connect_id": to_int_safe(op.get("connect")),
                        "client_id": to_int_safe(op.get("client")) or to_int_safe(op_key),
                        "ts_vrsrequest_us": op["ts_vrsrequest"].isoformat(timespec="microseconds"),
                        "ts_vrsresponse_us": op["ts_vrsresponse"].isoformat(timespec="microseconds"),
                    }

                prev_ts = None
                for ev_data in op["events"]:
                    ts = ev_data["ts"]
                    meta = ev_data["meta"]
                    props_inner = ev_data["props"]
                    event_string = ev_data["text"]
                    space = int((ts - prev_ts).total_seconds() * 1_000_000 - int(meta["duration"])) if prev_ts else 0
                    prev_ts = ts

                    row = {
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

        def start_operation(op_key: str, session_value: str | None, connect_value: str | None) -> None:
            """
            Инициализирует новую операцию (VRSREQUEST/SESN Start): кладёт первую точку и обновляет props.
            extra_fields позволяет добавить специфичные поля без дублирования кода.
            """
            if op_key in active_ops:
                return
            active_ops[op_key] = {
                "session": session_value,
                "connect": connect_value,
                "ts_vrsrequest": ts_event,
                "events": [{"meta": event_meta, "props": props, "text": event_text, "ts": ts_event}],
            }
            update_op_props(active_ops[op_key], props)

        props = extract_props(event_text)
        client_id = props.get("t:clientID")
        session_id = props.get("SessionID")
        connect_id = props.get("t:connectID")
                  
        if event_name == "VRSREQUEST":
            if client_id in pending_ops:
                del pending_ops[client_id]
            start_operation(client_id, session_id, connect_id)

        elif event_name == "VRSRESPONSE":
            if client_id in active_ops:
                complete_operation(client_id)

        elif event_name == "CALL":
            pending = pending_ops.get(client_id)
            if pending:
                session_id_val = to_int_safe(props.get("SessionID"))
                if session_id_val == 0:
                    session_id_val = to_int_safe(pending.get("session_id"))
                batch_calls.append({
                    "event_name": event_name,
                    "duration_us": to_int_safe(event_meta.get("duration")),
                    "session_id": session_id_val,
                    "client_id": to_int_safe(props.get("t:clientID")),
                    "cpu_time": extract_metric_value(event_text, "CpuTime"),
                    "memory": extract_metric_value(event_text, "Memory"),
                    "memory_peak": extract_metric_value(event_text, "MemoryPeak"),
                    "connect_id": to_int_safe(pending.get("connect_id")),
                    "ts_vrsrequest_us": pending.get("ts_vrsrequest_us"),
                    "ts_vrsresponse_us": pending.get("ts_vrsresponse_us"),
                })
                batch_events.append({
                    "session_id": session_id_val,
                    "connect_id": to_int_safe(pending.get("connect_id")),
                    "client_id": to_int_safe(props.get("t:clientID")),
                    "user": props.get("Usr"),
                    "ts_vrsrequest_us": pending.get("ts_vrsrequest_us"),
                    "ts_vrsresponse_us": pending.get("ts_vrsresponse_us"),
                    "ts_event_us": ts_event.isoformat(timespec="microseconds"),
                    "ts_event": ts_event.replace(microsecond=0).isoformat(),
                    "event_name": event_name,
                    "duration_us": to_int_safe(event_meta.get("duration")),
                    "space_us": 0,
                    "event_string": event_text,
                    "context": props.get("Context"),
                })
                del pending_ops[client_id]

        elif event_name == "SESN":
           
            func = props.get("Func")
            appl = props.get("Appl")
            session_id = props.get("Nmb")

            if func == "Start" and appl == "BackgroundJob":
                start_operation(session_id, session_id, connect_id)

            elif func == "Finish" and session_id in active_ops: 
                complete_operation(session_id)

        else:
            if client_id in active_ops:
                op = active_ops[client_id]
            elif session_id in active_ops:
                op = active_ops[session_id]
            else:
                return            
            update_op_props(op, props)
            op["events"].append({"meta": event_meta, "props": props, "text": event_text, "ts": ts_event})

        if len(batch_events) >= BATCH_SIZE:
            insert_batch(TABLE_OPERATIONS, batch_ops)
            insert_batch(TABLE_EVENTS, batch_events)
            insert_batch(TABLE_EVENT_STATS, batch_event_stats)
            insert_batch(TABLE_CALL_OPS, batch_calls)
            batch_events.clear()
            batch_ops.clear()
            batch_event_stats.clear()
            batch_calls.clear()

    total_hours = len(by_hour)
    for hour_idx, (hour_key, hour_files) in enumerate(sorted(by_hour.items()), start=1):
        print(f"\n[INFO] Час {hour_key}: файлов {len(hour_files)} (rphost+rmngr)")
        logger.info(f"Начало обработки часа {hour_key}: {len(hour_files)} файлов")

        # Прогресс-бар по строкам всех файлов часа
        total_lines_hour = sum(get_file_line_count(fp) for fp in hour_files)
        with tqdm(
            total=total_lines_hour if total_lines_hour > 0 else None,
            desc=f"Час {hour_idx}/{total_hours}",
            unit="строк",
            unit_scale=True
        ) as pbar:
            lines_since_update = 0
            for ts_event, event_meta, event_text, _ in iter_read_events(hour_files):
                if not event_meta:
                    continue
                handle_event(event_meta, event_text, ts_event)

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

    if batch_events or batch_ops or batch_event_stats or batch_calls:
        insert_batch(TABLE_EVENTS, batch_events)
        insert_batch(TABLE_OPERATIONS, batch_ops)
        insert_batch(TABLE_EVENT_STATS, batch_event_stats)
        insert_batch(TABLE_CALL_OPS, batch_calls)
        logger.info(f"Отправлена финальная пачка: {len(batch_events)} событий, {len(batch_ops)} операций")

    completion_msg = f"Готово: обработано {total_operations} операций"
    logger.info(completion_msg)
    print(f"[INFO] {completion_msg}")
    
if __name__ == "__main__":
    logger.info(f"Запуск скрипта trace-vrs. Лог сохраняется в: {log_file}")
    print(f"[INFO] Лог сохраняется в: {log_file}")

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
