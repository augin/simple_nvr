import argparse
import time
import os
import yaml
from datetime import datetime, timedelta
import subprocess
import schedule
from pathlib import Path
import signal
import logging
from typing import Dict, Any, Optional, List, Tuple
import threading
from urllib.parse import parse_qs

# Импорт для динамического отслеживания изменений в конфигурации
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Импорт для создания веб‑интерфейса
from flask import Flask, jsonify

# ===================== Настройка логирования =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ===================== Вспомогательные функции для парсинга fragment в URI =====================
def _parse_fragment(fragment: str) -> Dict[str, List[str]]:
    """
    Парсит часть URI после '#' как query-string и возвращает dict как parse_qs.
    """
    return parse_qs(fragment, keep_blank_values=True)


def _pick_first(values: List[str]) -> Optional[str]:
    return values[0] if values else None


def _parse_number(value: Optional[str]) -> Optional[float]:
    """
    Пытается распарсить строку как число (поддерживаются целые и дробные).
    Возвращает float или None.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_rec_from_uri(raw: str) -> float:
    """
    Разбирает URI потока и возвращает числовой параметр rec / rec_size / record_size в гигабайтах (GB).
    """
    if "#" in raw:
        _, frag = raw.split("#", 1)
    else:
        frag = ""

    if not frag:
        return 0.0

    params = _parse_fragment(frag)
    rec_size_val = _pick_first(params.get("rec_size", [])) or _pick_first(params.get("record_size", []))
    rec_val = _pick_first(params.get("rec", []))

    n = _parse_number(rec_size_val)
    if n is None:
        n = _parse_number(rec_val)

    if n is not None:
        return max(0.0, n)
    else:
        # если были параметры, но нечисловые — логируем предупреждение
        if (rec_size_val is not None and rec_size_val != "") or (rec_val is not None and rec_val != ""):
            logger.warning(
                "Параметр записи задан в URI, но не содержит числового значения (ожидается GB): rec=%r rec_size=%r",
                rec_val,
                rec_size_val,
            )
        return 0.0

# ===================== Функция чтения конфигурации =====================
def read_dvr_config(config_file: str) -> Dict[str, Any]:
    """
    Читает основной YAML-конфигурационный файл.
    """
    try:
        with open(config_file, 'r') as file:
            config_data = yaml.safe_load(file)
        return config_data
    except Exception as e:
        logger.error(f"Ошибка чтения конфигурационного файла {config_file}: {e}")
        raise

# ===================== Основной класс записи =====================
class VideoRecorder:
    """
    Класс для записи видеопотоков, управления процессами, очистки хранилища и автоматического перезапуска.

    Поведение изменено так, чтобы расписание запусков (каждую 10-ю минуту) всегда
    запускало новые записи для всех камер с quota > 0: если для камеры уже запущен процесс,
    он будет корректно завершён (TERM -> KILL при необходимости) и заменён новым процессом.
    Это обеспечивает, что запись начинается ровно в указанную минуту.
    """
    def __init__(self, dvr_config_file: str) -> None:
        self.config_file: str = dvr_config_file
        self.config: Dict[str, Any] = read_dvr_config(dvr_config_file)
        self.base_dir: Path = Path(self.config['base_dir'])
        self.stream_server: str = self.config['stream_server']
        self.target_size_gb: float = float(self.config.get('target_size_gb', 0.0))
        self.go2rtc_config_path: Path = Path(self.config['go2rtc_config_path'])
        # Кэш конфига для go2rtc
        self.go2rtc_config: Dict[str, Any] = self.load_go2rtc_config()
        # Словарь с quota (GB) для камер по имени: {camera_name: quota_gb}
        self.camera_quotas_gb: Dict[str, float] = self._build_camera_quotas(self.go2rtc_config)
        # Словарь активных процессов; для каждого потока хранится одна запись
        # {"process": Popen, "start_time": datetime, "duration": int}
        self.active_processes: Dict[str, Dict[str, Any]] = {}
        self.process_lock = threading.Lock()

        # Для фона очистки: флаг и блокировка, чтобы не стартовать несколько clean-циклов одновременно
        self._cleanup_lock = threading.Lock()
        self._cleanup_in_progress = False
        self._cleanup_thread: Optional[threading.Thread] = None

        logger.debug("Initialized VideoRecorder; streams keys: %s", list(self.go2rtc_config.get('streams', {}).keys()))

    def load_go2rtc_config(self) -> Dict[str, Any]:
        """
        Загружает и кэширует конфигурацию go2rtc.
        """
        try:
            with self.go2rtc_config_path.open('r') as file:
                config_data = yaml.safe_load(file) or {}
            return config_data
        except Exception as e:
            logger.error(f"Ошибка чтения go2rtc-конфигурации {self.go2rtc_config_path}: {e}")
            raise

    def _build_camera_quotas(self, go2rtc_cfg: Dict[str, Any]) -> Dict[str, float]:
        """
        Постройка словаря квот (GB) для камер на основе go2rtc-конфигурации.
        """
        quotas: Dict[str, float] = {}
        streams = go2rtc_cfg.get('streams', {}) or {}
        for cam_name, uris in streams.items():
            if isinstance(uris, str):
                uris_list = [uris]
            else:
                uris_list = list(uris or [])

            quota = 0.0
            for raw_uri in uris_list:
                try:
                    q = parse_rec_from_uri(raw_uri)
                    if q > 0.0:
                        quota = q
                        break
                except Exception:
                    logger.exception("Ошибка при парсинге URI %r для камеры %s", raw_uri, cam_name)
            quotas[cam_name] = quota
        logger.debug("camera_quotas_gb: %s", quotas)
        return quotas

    def reload_config(self) -> None:
        """
        Перезагружает основную конфигурацию и обновляет параметры.
        """
        try:
            new_config = read_dvr_config(self.config_file)
            self.config = new_config
            self.base_dir = Path(new_config['base_dir'])
            self.stream_server = new_config['stream_server']
            self.target_size_gb = float(new_config.get('target_size_gb', 0.0))
            self.go2rtc_config_path = Path(new_config['go2rtc_config_path'])
            self.go2rtc_config = self.load_go2rtc_config()
            # Перестроим квоты
            self.camera_quotas_gb = self._build_camera_quotas(self.go2rtc_config)
            logger.info("Основная конфигурация успешно перезагружена.")
        except Exception as e:
            logger.error(f"Ошибка перезагрузки конфигурации: {e}")

    def _parse_raw_uri_candidate(self, raw: str) -> Optional[str]:
        """
        Убирает фрагмент после '#' и возвращает строку или None.
        """
        if not isinstance(raw, str):
            return None
        return raw.split('#', 1)[0].strip()

    def _choose_input_url(self, stream_name: str) -> Tuple[str, Optional[str]]:
        """
        Выбирает URL для ffmpeg для заданного stream_name на основе go2rtc config.
        Возвращает (input_url, used_raw_uri) where used_raw_uri is raw value used (if any).
        Правила:
          - если в go2rtc streams для имени задана строка или список, пытаемся взять первую строку
            и если она выглядит как URI (rtsp://, rtsps://, http://, https://) — используем её
          - иначе используем fallback self.stream_server + '/' + stream_name
        """
        streams = self.go2rtc_config.get('streams', {}) or {}
        raw = streams.get(stream_name)
        used_raw = None

        candidate = None
        if isinstance(raw, str):
            candidate = self._parse_raw_uri_candidate(raw)
            used_raw = raw
        elif isinstance(raw, (list, tuple)):
            for item in raw:
                if isinstance(item, str):
                    candidate = self._parse_raw_uri_candidate(item)
                    used_raw = item
                    break

        if isinstance(candidate, str):
            lc = candidate.lower()
            if lc.startswith("rtsp://") or lc.startswith("rtsps://") or lc.startswith("http://") or lc.startswith("https://"):
                return candidate, used_raw

        # fallback
        fallback = f"{self.stream_server.rstrip('/')}/{stream_name}"
        return fallback, None

    def _start_recording_for_stream(self, stream_name: str, duration: int) -> Optional[subprocess.Popen]:
        """
        Запускает ffmpeg для конкретного потока, используя выбранный input URL.
        Возвращает Popen если процесс успешно запущен и жив, иначе None.
        """
        now = datetime.now()
        timestamp = now.strftime("%H-%M")
        date_path = Path(now.strftime("%Y")) / now.strftime("%m") / now.strftime("%d")
        directory = self.base_dir / stream_name / date_path
        directory.mkdir(parents=True, exist_ok=True)
        output_file = directory / f"{timestamp}.mp4"

        input_url, used_raw = self._choose_input_url(stream_name)
        logger.debug("Stream %s: used_raw=%r input_url=%s output=%s", stream_name, used_raw, input_url, output_file)

        command = [
            'ffmpeg',
            '-hide_banner',
            '-loglevel', 'error',
            '-threads', '2',
            '-rtsp_transport', 'tcp',
            '-avoid_negative_ts', 'make_zero',
            '-fflags', '+nobuffer+genpts+discardcorrupt',
            '-flags', 'low_delay',
            '-use_wallclock_as_timestamps', '1',
            '-i', input_url,
            '-reset_timestamps', '1',
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-t', str(duration),
            str(output_file)
        ]

        try:
            proc = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                preexec_fn=os.setsid
            )
        except Exception as e:
            logger.exception("Не удалось запустить ffmpeg для %s (command: %s): %s", stream_name, command, e)
            return None

        # Короткая задержка — если процесс сразу падает, не добавляем его в active_processes.
        time.sleep(0.25)
        if proc.poll() is not None:
            logger.warning("ffmpeg для %s сразу завершился с кодом %s (input=%s).", stream_name, proc.poll(), input_url)
            return None

        logger.info("Начата запись для %s -> %s (PID %s)", stream_name, output_file, proc.pid)
        return proc

    def _prune_finished_processes(self) -> None:
        """
        Удаляет завершённые процессы из active_processes.
        """
        with self.process_lock:
            for stream in list(self.active_processes.keys()):
                info = self.active_processes.get(stream)
                if not info:
                    self.active_processes.pop(stream, None)
                    continue
                proc = info.get("process")
                try:
                    if proc is None or proc.poll() is not None:
                        self.active_processes.pop(stream, None)
                        logger.debug("Pruned finished process entry for stream %s", stream)
                except Exception:
                    logger.exception("Ошибка при проверке статуса процесса для %s", stream)
                    self.active_processes.pop(stream, None)

    def _terminate_existing_process(self, stream_name: str, proc: subprocess.Popen, grace: float = 1.0) -> None:
        """
        Попытаться корректно завершить существующий процесс записи (TERM, подождать, затем KILL).
        """
        try:
            logger.info("Завершаю текущую запись %s (PID %s) чтобы запустить новую по расписанию.", stream_name, proc.pid)
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            logger.warning("Процесс %s уже отсутствует при попытке TERM.", stream_name)
        except Exception:
            logger.exception("Ошибка при отправке SIGTERM процессу %s", stream_name)

        # даём время на завершение
        t0 = time.time()
        while time.time() - t0 < grace:
            if proc.poll() is not None:
                return
            time.sleep(0.05)

        # если не завершился — принудительно убиваем
        try:
            if proc.poll() is None:
                logger.info("SIGKILL процессу %s (PID %s) — не завершился после TERM.", stream_name, proc.pid)
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            logger.warning("Процесс %s уже отсутствует при попытке KILL.", stream_name)
        except Exception:
            logger.exception("Ошибка при отправке SIGKILL процессу %s", stream_name)

    def record_streams(self, duration: int) -> Dict[str, Any]:
        """
        Запускает запись для потоков из go2rtc-конфигурации, но только для тех, у кого quota (rec in GB) > 0.
        Если уже есть активный процесс для потока — он будет корректно завершён и заменён новым,
        чтобы гарантировать старт записи ровно по расписанию.
        """
        now = datetime.now()
        streams = self.go2rtc_config.get('streams', {}) or {}

        try:
            self._prune_finished_processes()
        except Exception:
            logger.exception("Ошибка при очистке завершённых процессов перед стартом записей.")

        logger.info("record_streams: total streams in go2rtc=%d duration=%d", len(streams), duration)
        started = 0
        for stream_name in streams.keys():
            try:
                quota = float(self.camera_quotas_gb.get(stream_name, 0.0))
                if quota <= 0.0:
                    logger.debug("Skip %s: quota=%s", stream_name, quota)
                    continue

                # Если уже есть живой процесс — завершаем его, чтобы запустить новый по расписанию
                with self.process_lock:
                    existing = self.active_processes.get(stream_name)
                    if existing:
                        proc_existing = existing.get("process")
                        if proc_existing and proc_existing.poll() is None:
                            # завершаем существующий корректно (TERM -> KILL) перед стартом нового
                            try:
                                self._terminate_existing_process(stream_name, proc_existing, grace=1.0)
                            except Exception:
                                logger.exception("Ошибка при завершении существующего процесса для %s", stream_name)
                            # удаляем запись (независимо от успеха) — мы заменим её ниже
                            self.active_processes.pop(stream_name, None)

                proc = self._start_recording_for_stream(stream_name, duration)
                if not proc:
                    logger.warning("Recording for %s was not started (ffmpeg failed or exited).", stream_name)
                    continue

                with self.process_lock:
                    self.active_processes[stream_name] = {
                        "process": proc,
                        "start_time": now,
                        "duration": duration
                    }
                started += 1
            except Exception as e:
                logger.exception("Ошибка при запуске записи для %s: %s", stream_name, e)

        logger.info("record_streams finished: started=%d of %d", started, len(streams))

        try:
            self._start_background_cleanup()
        except Exception:
            logger.exception("Не удалось запустить фоновую очистку директорий.")

        with self.process_lock:
            return self.active_processes.copy()

    def clean_camera_folders(self, min_age_seconds: int = 3600) -> None:
        """
        Удаляет старые файлы и пустые директории. Использует per-camera quota (GB), если задана.
        """
        now = time.time()
        for camera_dir in self.base_dir.iterdir():
            if camera_dir.is_dir():
                cam_name = camera_dir.name
                cam_quota_gb = float(self.camera_quotas_gb.get(cam_name, self.target_size_gb))
                try:
                    size_bytes = sum(f.stat().st_size for f in camera_dir.glob('**/*') if f.is_file())
                    size_gb = size_bytes / (1024 ** 3)
                except Exception as e:
                    logger.error("Ошибка при расчёте размера %s: %s", camera_dir, e)
                    continue

                space_to_free_gb = size_gb - cam_quota_gb
                if space_to_free_gb > 0:
                    logger.info("Очистка %s: нужно освободить %.3f ГБ (quota %.3f ГБ).", camera_dir, space_to_free_gb, cam_quota_gb)
                    try:
                        files = sorted((f for f in camera_dir.glob('**/*') if f.is_file()), key=lambda f: f.stat().st_mtime)
                    except Exception as e:
                        logger.error("Ошибка сортировки файлов в %s: %s", camera_dir, e)
                        continue

                    for file in files:
                        if space_to_free_gb <= 0:
                            break
                        try:
                            file_size_gb = file.stat().st_size / (1024 ** 3)
                            file.unlink()
                            logger.info("Удалён файл %s размером %.3f ГБ.", file, file_size_gb)
                            space_to_free_gb -= file_size_gb
                        except Exception as e:
                            logger.error("Ошибка при удалении файла %s: %s", file, e)

        for root, dirs, _ in os.walk(self.base_dir, topdown=False):
            for dir_name in dirs:
                dir_path = Path(root) / dir_name
                try:
                    folder_age = now - dir_path.stat().st_ctime
                    if folder_age < min_age_seconds:
                        continue
                    if not any(dir_path.iterdir()):
                        dir_path.rmdir()
                        logger.info("Удалена пустая директория: %s", dir_path)
                except Exception as e:
                    logger.error("Ошибка при удалении директории %s: %s", dir_path, e)

    def _start_background_cleanup(self, min_age_seconds: int = 3600) -> None:
        with self._cleanup_lock:
            if self._cleanup_in_progress:
                logger.debug("Очистка уже выполняется; новый фоновый запуск не требуется.")
                return
            self._cleanup_in_progress = True

            def _worker():
                try:
                    logger.info("Запущена фоновая очистка директорий.")
                    try:
                        self.clean_camera_folders(min_age_seconds=min_age_seconds)
                    except Exception as e:
                        logger.exception("Ошибка в фоновом процессе очистки: %s", e)
                    logger.info("Фоновая очистка директорий завершена.")
                finally:
                    with self._cleanup_lock:
                        self._cleanup_in_progress = False

            t = threading.Thread(target=_worker, daemon=True)
            self._cleanup_thread = t
            t.start()

    def cleanup_processes(self) -> None:
        """
        Корректно завершает все активные процессы.
        """
        with self.process_lock:
            for stream, info in list(self.active_processes.items()):
                proc = info["process"]
                if proc.poll() is None:
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                        logger.info("Завершён процесс для %s.", stream)
                    except ProcessLookupError:
                        logger.warning("Процесс %s уже завершён.", stream)
                    except Exception as e:
                        logger.error("Ошибка при завершении процесса %s: %s", stream, e)
                self.active_processes.pop(stream, None)

    def handle_termination(self, signum: int, frame) -> None:
        """
        Обработчик сигналов завершения.
        """
        signal_names = {signal.SIGINT: "SIGINT", signal.SIGTERM: "SIGTERM", signal.SIGHUP: "SIGHUP"}
        logger.info("Получен сигнал %s, завершаю работу.", signal_names.get(signum, str(signum)))
        self.cleanup_processes()
        time.sleep(5)
        with self.process_lock:
            for stream, info in list(self.active_processes.items()):
                proc = info.get("process")
                if proc and proc.poll() is None:
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        logger.info("Принудительно завершён процесс для %s.", stream)
                    except ProcessLookupError:
                        logger.warning("Процесс %s уже завершён.", stream)
                    except Exception as e:
                        logger.error("Ошибка при принудительном завершении процесса %s: %s", stream, e)
                self.active_processes.pop(stream, None)
        schedule.clear()
        exit(0)

    def monitor_processes(self, check_interval: int = 5, restart_threshold: int = 5) -> None:
        """
        Монитор активных процессов: если процесс умер преждевременно — пытаемся перезапустить.
        """
        while True:
            try:
                self._prune_finished_processes()
            except Exception:
                logger.exception("Ошибка при периодическом prune перед мониторингом.")

            with self.process_lock:
                for stream, info in list(self.active_processes.items()):
                    proc = info.get("process")
                    start_time = info.get("start_time")
                    duration = info.get("duration")
                    planned_end = start_time + timedelta(seconds=duration)
                    now = datetime.now()
                    try:
                        finished = proc.poll() is not None
                    except Exception:
                        finished = True

                    if finished:
                        remaining = (planned_end - now).total_seconds()
                        if remaining > restart_threshold:
                            logger.warning("Процесс для %s завершился преждевременно. Перезапуск на оставшиеся %d сек.", stream, int(remaining))
                            try:
                                new_proc = self._start_recording_for_stream(stream, int(remaining))
                                if new_proc:
                                    self.active_processes[stream] = {
                                        "process": new_proc,
                                        "start_time": datetime.now(),
                                        "duration": int(remaining)
                                    }
                                else:
                                    # удалим запись — не смогли перезапустить
                                    self.active_processes.pop(stream, None)
                            except Exception as e:
                                logger.error("Ошибка перезапуска записи для %s: %s", stream, e)
                        else:
                            logger.info("Запись для %s завершена корректно.", stream)
                            self.active_processes.pop(stream, None)
            time.sleep(check_interval)

# ===================== Watchdog: Отслеживание изменений конфигурации =====================
class ConfigChangeHandler(FileSystemEventHandler):
    """
    Обработчик событий изменения файлов конфигурации.
    """
    def __init__(self, recorder: VideoRecorder) -> None:
        self.recorder = recorder

    def on_modified(self, event) -> None:
        # Если изменён основной конфигурационный файл – перезагружаем его
        if event.src_path == str(Path(self.recorder.config_file).resolve()):
            logger.info("Обнаружено изменение основного конфигурационного файла, перезагружаем...")
            self.recorder.reload_config()
        # Если изменён конфиг go2rtc – перечитываем его и перестраиваем квоты
        elif event.src_path == str(self.recorder.go2rtc_config_path.resolve()):
            logger.info("Обнаружено изменение go2rtc-конфигурационного файла, перезагружаем...")
            try:
                new_go2rtc_config = self.recorder.load_go2rtc_config()
                self.recorder.go2rtc_config = new_go2rtc_config
                self.recorder.camera_quotas_gb = self.recorder._build_camera_quotas(new_go2rtc_config)
                logger.info("Конфигурация go2rtc успешно перезагружена и квоты обновлены.")
            except Exception as e:
                logger.error(f"Ошибка перезагрузки go2rtc-конфигурации: {e}")

def start_config_observer(recorder: VideoRecorder) -> Observer:
    """
    Запускает наблюдателя watchdog для конфигурационного файла.
    """
    event_handler = ConfigChangeHandler(recorder)
    observer = Observer()
    config_dir = str(Path(recorder.config_file).parent)
    observer.schedule(event_handler, path=config_dir, recursive=False)
    observer.start()
    logging.info(f"Наблюдение за изменениями в {config_dir} запущено.")
    return observer

# ===================== Веб-интерфейс (дашборд) =====================
def create_dashboard_app(recorder: VideoRecorder) -> Flask:
    """
    Создает Flask‑приложение для отображения статуса записи.
    """
    app = Flask(__name__)

    @app.route('/')
    def index():
        with recorder.process_lock:
            active = {
                stream: {
                    "pid": info["process"].pid,
                    "status": "running" if info["process"].poll() is None else "stopped",
                    "start_time": info["start_time"].isoformat(),
                    "duration": info["duration"]
                }
                for stream, info in recorder.active_processes.items()
            }
        # Покажем также текущие квоты камер
        return jsonify({
            "active_processes": active,
            "config": recorder.config,
            "go2rtc_camera_quotas_gb": recorder.camera_quotas_gb,
            "cleanup_in_progress": recorder._cleanup_in_progress
        })

    return app

def run_flask_app(app: Flask, host: str = "0.0.0.0", port: int = 5000) -> None:
    """
    Запускает Flask‑сервер.
    """
    app.run(host=host, port=port, use_reloader=False)

# ===================== Основная функция =====================
def main() -> None:
    parser = argparse.ArgumentParser(description='Запись видеопотоков.')
    parser.add_argument('--config_file', type=str, help='Путь к файлу конфигурации', required=True)
    args = parser.parse_args()
    dvr_config_file = args.config_file

    recorder = VideoRecorder(dvr_config_file)

    # Запуск наблюдения за изменениями конфигурации (watchdog)
    observer = start_config_observer(recorder)

    # Запуск веб-интерфейса в отдельном потоке
    dashboard_app = create_dashboard_app(recorder)
    flask_thread = threading.Thread(target=run_flask_app, args=(dashboard_app,), daemon=True)
    flask_thread.start()
    logging.info("Веб-интерфейс запущен на порту 5000.")

    # Запуск авто-мониторинга и перезапуска процессов в отдельном потоке
    monitor_thread = threading.Thread(target=recorder.monitor_processes, daemon=True)
    monitor_thread.start()
    logging.info("Автоматический мониторинг и перезапуск потоков включён.")

    # Обработчик сигналов завершения работы
    def termination_handler(signum, frame) -> None:
        recorder.handle_termination(signum, frame)
        observer.stop()

    signal.signal(signal.SIGINT, termination_handler)
    signal.signal(signal.SIGTERM, termination_handler)
    signal.signal(signal.SIGHUP, termination_handler)

    # Планирование записи: каждые 10 минут (на 00, 10, 20, 30, 40, 50 минут часа)
    duration = 607  # 10 минут 7 секунд
    for minute in ['00', '10', '20', '30', '40', '50']:
        schedule.every().hour.at(f":{minute}").do(recorder.record_streams, duration)

    # Расчёт оставшегося времени до следующего интервала
    now = datetime.now()
    next_minute = ((now.minute // 10 + 1) * 10) % 60
    if next_minute <= now.minute:
        next_run = datetime(now.year, now.month, now.day, now.hour, next_minute) + timedelta(hours=1)
    else:
        next_run = datetime(now.year, now.month, now.day, now.hour, next_minute)
    remaining_time = (next_run - now).total_seconds()

    logging.info(f"Начальная запись: оставшееся время до интервала {remaining_time:.0f} сек.")
    recorder.record_streams(int(remaining_time))

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except Exception as e:
        logging.error(f"Ошибка в основном цикле: {e}")
        recorder.handle_termination(signal.SIGTERM, None)
        observer.stop()

if __name__ == '__main__':
    main()
