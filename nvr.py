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
from typing import Dict, Any, Optional, List
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
    Логика:
      - смотрим fragment после '#'
      - приоритет: rec_size / record_size > rec
      - если значение числовое (целое/дробное) — возвращаем его (>=0)
      - иначе возвращаем 0.0
    Примеры:
      rtsp://.../stream#rec=2       -> 2.0 (GB)
      rtsp://.../stream#rec_size=1.5 -> 1.5 (GB)
      rtsp://.../stream#rec=true    -> 0.0 (нечисловое значение игнорируется)
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
            logging.warning(
                "Параметр записи задан в URI, но не содержит числового значения (ожидается GB): rec=%r rec_size=%r",
                rec_val,
                rec_size_val,
            )
        return 0.0

# ===================== Функция чтения конфигурации =====================
def read_dvr_config(config_file: str) -> Dict[str, Any]:
    """
    Читает основной YAML-конфигурационный файл.

    :param config_file: Путь к конфигурационному файлу.
    :return: Словарь настроек.
    """
    try:
        with open(config_file, 'r') as file:
            config_data = yaml.safe_load(file)
        return config_data
    except Exception as e:
        logging.error(f"Ошибка чтения конфигурационного файла {config_file}: {e}")
        raise

# ===================== Основной класс записи =====================
class VideoRecorder:
    """
    Класс для записи видеопотоков, управления процессами, очистки хранилища и автоматического перезапуска.

    Изменения:
    - читаем go2rtc-конфигурацию и парсим для каждой камеры параметр rec (в GB).
      Этот числовой объём выступает и как "флаг записи": если > 0 — запись включена для камеры.
    - при очистке директорий учитывается quota (rec в GB) для конкретной камеры, если задана,
      иначе используется глобальная target_size_gb.
    - при старте записи (record_streams) запускаем запись только для тех камер, у которых quota > 0.
    - очистка запускается в фоне (в отдельном потоке), чтобы не блокировать старт записи.
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
        # Словарь активных процессов; теперь для каждого потока сохраняем словарь с тремя полями:
        # "process" – объект subprocess.Popen,
        # "start_time" – время запуска,
        # "duration" – планируемая длительность записи (в секундах).
        self.active_processes: Dict[str, Dict[str, Any]] = {}
        self.process_lock = threading.Lock()

        # Для фона очистки: флаг и блокировка, чтобы не стартовать несколько clean-циклов одновременно
        self._cleanup_lock = threading.Lock()
        self._cleanup_in_progress = False
        self._cleanup_thread: Optional[threading.Thread] = None

    def load_go2rtc_config(self) -> Dict[str, Any]:
        """
        Загружает и кэширует конфигурацию go2rtc.

        :return: Словарь настроек потоков.
        """
        try:
            with self.go2rtc_config_path.open('r') as file:
                config_data = yaml.safe_load(file) or {}
            return config_data
        except Exception as e:
            logging.error(f"Ошибка чтения go2rtc-конфигурации {self.go2rtc_config_path}: {e}")
            raise

    def _build_camera_quotas(self, go2rtc_cfg: Dict[str, Any]) -> Dict[str, float]:
        """
        Постройка словаря квот (GB) для камер на основе go2rtc-конфигурации.
        Поддерживается следующие формы в streams:
          camera: "rtsp://...#rec=2"
          camera:
            - "rtsp://...#rec=2"
            - "rtsp://...#rec=0"
        Правило:
          - если у камеры несколько URI — берём первое числовое значение rec/rec_size/record_size (GB),
            которое встречается; если ни у одного нет — 0.0
        """
        quotas: Dict[str, float] = {}
        streams = go2rtc_cfg.get('streams', {}) or {}
        for cam_name, uris in streams.items():
            # uris может быть строкой или списком
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
                    logging.exception("Ошибка при парсинге URI %r для камеры %s", raw_uri, cam_name)
            quotas[cam_name] = quota
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
            logging.info("Основная конфигурация успешно перезагружена.")
        except Exception as e:
            logging.error(f"Ошибка перезагрузки конфигурации: {e}")

    def clean_camera_folders(self, min_age_seconds: int = 3600) -> None:
        """
        Удаляет старые файлы и пустые директории. Каталоги, созданные менее min_age_seconds назад, не трогаются.

        Использует per-camera quota (GB), если она задана в go2rtc конфиге; иначе fallback на self.target_size_gb.

        :param min_age_seconds: Минимальный возраст (сек) пустой директории для её удаления.
        """
        now = time.time()

        # 1. Очистка файлов: если размер каталога превышает лимит, удаляются самые старые файлы.
        for camera_dir in self.base_dir.iterdir():
            if camera_dir.is_dir():
                cam_name = camera_dir.name
                # получаем quota для этой камеры; если не задана — используем глобальную target_size_gb
                cam_quota_gb = float(self.camera_quotas_gb.get(cam_name, self.target_size_gb))
                try:
                    size_bytes = sum(f.stat().st_size for f in camera_dir.glob('**/*') if f.is_file())
                    size_gb = size_bytes / (1024 ** 3)
                except Exception as e:
                    logging.error(f"Ошибка при расчёте размера {camera_dir}: {e}")
                    continue

                space_to_free_gb = size_gb - cam_quota_gb
                if space_to_free_gb > 0:
                    logging.info(f"Очистка {camera_dir}: нужно освободить {space_to_free_gb:.3f} ГБ (quota {cam_quota_gb:.3f} ГБ).")
                    try:
                        files = sorted((f for f in camera_dir.glob('**/*') if f.is_file()), key=lambda f: f.stat().st_mtime)
                    except Exception as e:
                        logging.error(f"Ошибка сортировки файлов в {camera_dir}: {e}")
                        continue

                    for file in files:
                        if space_to_free_gb <= 0:
                            break
                        try:
                            file_size_gb = file.stat().st_size / (1024 ** 3)
                            file.unlink()
                            logging.info(f"Удалён файл {file} размером {file_size_gb:.3f} ГБ.")
                            space_to_free_gb -= file_size_gb
                        except Exception as e:
                            logging.error(f"Ошибка при удалении файла {file}: {e}")

        # 2. Удаление пустых директорий (если возраст больше min_age_seconds)
        for root, dirs, _ in os.walk(self.base_dir, topdown=False):
            for dir_name in dirs:
                dir_path = Path(root) / dir_name
                try:
                    folder_age = now - dir_path.stat().st_ctime
                    if folder_age < min_age_seconds:
                        continue
                    if not any(dir_path.iterdir()):
                        dir_path.rmdir()
                        logging.info(f"Удалена пустая директория: {dir_path}")
                except Exception as e:
                    logging.error(f"Ошибка при удалении директории {dir_path}: {e}")

    def _start_background_cleanup(self, min_age_seconds: int = 3600) -> None:
        """
        Запускает clean_camera_folders в отдельном потоке, если в данный момент очистка не выполняется.
        Это позволяет не блокировать record_streams.
        """
        # Быстро проверяем и ставим флаг под блокировкой
        with self._cleanup_lock:
            if self._cleanup_in_progress:
                logging.debug("Очистка уже выполняется; новый фоновый запуск не требуется.")
                return
            self._cleanup_in_progress = True

            def _worker():
                try:
                    logging.info("Запущена фоновая очистка директорий.")
                    try:
                        self.clean_camera_folders(min_age_seconds=min_age_seconds)
                    except Exception as e:
                        logging.exception("Ошибка в фоновом процессе очистки: %s", e)
                    logging.info("Фоновая очистка директорий завершена.")
                finally:
                    with self._cleanup_lock:
                        self._cleanup_in_progress = False

            t = threading.Thread(target=_worker, daemon=True)
            self._cleanup_thread = t
            t.start()

    def _start_recording_for_stream(self, stream_name: str, duration: int) -> subprocess.Popen:
        """
        Вспомогательный метод для запуска записи для одного потока.

        :param stream_name: Имя потока.
        :param duration: Длительность записи (в секундах).
        :return: Запущенный процесс.
        """
        now = datetime.now()
        timestamp = now.strftime("%H-%M")
        date_path = Path(now.strftime("%Y")) / now.strftime("%m") / now.strftime("%d")
        directory = self.base_dir / stream_name / date_path
        directory.mkdir(parents=True, exist_ok=True)
        output_file = directory / f"{timestamp}.mp4"

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
            '-i', f"{self.stream_server}/{stream_name}",
            '-reset_timestamps', '1',
            '-strftime', '1',
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-strict', 'experimental',
            '-t', str(duration),
            str(output_file)
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid
        )
        logging.info(f"Начата запись для {stream_name} в файл {output_file} на {duration} сек (PID {process.pid}).")
        return process

    def record_streams(self, duration: int) -> Dict[str, Any]:
        """
        Запускает запись для потоков из go2rtc-конфигурации, но только для тех, у кого quota (rec in GB) > 0.

        :param duration: Длительность записи (сек).
        :return: Словарь с информацией по запущенным процессам.
        """
        now = datetime.now()
        streams = self.go2rtc_config.get('streams', {}) or {}
        for stream_name in streams.keys():
            try:
                quota = float(self.camera_quotas_gb.get(stream_name, 0.0))
                if quota <= 0.0:
                    logging.debug(f"Пропуск записи для {stream_name}: quota не задана или равна 0.")
                    continue

                # если уже есть активный процесс для потока — пропускаем запуск дубликата
                with self.process_lock:
                    if stream_name in self.active_processes:
                        proc_info = self.active_processes[stream_name]
                        proc = proc_info.get("process")
                        if proc and proc.poll() is None:
                            logging.debug(f"Процесс для {stream_name} уже запущен (PID {proc.pid}), пропускаем новый запуск.")
                            continue

                proc = self._start_recording_for_stream(stream_name, duration)
                with self.process_lock:
                    # Сохраняем дополнительные данные: время запуска и планируемую длительность.
                    self.active_processes[stream_name] = {
                        "process": proc,
                        "start_time": now,
                        "duration": duration
                    }
            except Exception as e:
                logging.error(f"Ошибка при запуске записи для {stream_name}: {e}")

        # Запускаем очистку в фоне, чтобы она не блокировала старт записи
        try:
            self._start_background_cleanup()
        except Exception:
            logging.exception("Не удалось запустить фоновую очистку директорий.")

        # Для удобства можно возвращать копию active_processes (но это необязательно)
        with self.process_lock:
            return self.active_processes.copy()

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
                        logging.info(f"Завершён процесс для {stream}.")
                    except ProcessLookupError:
                        logging.warning(f"Процесс {stream} уже завершён.")
                    except Exception as e:
                        logging.error(f"Ошибка при завершении процесса {stream}: {e}")
                self.active_processes.pop(stream, None)

    def handle_termination(self, signum: int, frame) -> None:
        """
        Обработчик сигналов завершения. Корректно завершает процессы и очищает планировщик.
        """
        signal_names = {signal.SIGINT: "SIGINT", signal.SIGTERM: "SIGTERM", signal.SIGHUP: "SIGHUP"}
        logging.info(f"Получен сигнал {signal_names.get(signum, str(signum))}, завершаю работу.")
        self.cleanup_processes()
        time.sleep(5)
        # Принудительное завершение оставшихся процессов
        with self.process_lock:
            for stream, info in list(self.active_processes.items()):
                proc = info["process"]
                if proc.poll() is None:
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        logging.info(f"Принудительно завершён процесс для {stream}.")
                    except ProcessLookupError:
                        logging.warning(f"Процесс {stream} уже завершён.")
                    except Exception as e:
                        logging.error(f"Ошибка при принудительном завершении процесса {stream}: {e}")
                    self.active_processes.pop(stream, None)
        schedule.clear()
        exit(0)

    def monitor_processes(self, check_interval: int = 5, restart_threshold: int = 5) -> None:
        """
        В фоне проверяет активные процессы. Если процесс закончился раньше завершения запланированного времени[...]
        перезапускает его на оставшуюся длительность.

        :param check_interval: Интервал проверки в секундах.
        :param restart_threshold: Если оставшееся время записи больше этого порога (сек), запускается повтор.
        """
        while True:
            with self.process_lock:
                for stream, info in list(self.active_processes.items()):
                    proc = info["process"]
                    start_time = info["start_time"]
                    duration = info["duration"]
                    planned_end = start_time + timedelta(seconds=duration)
                    now = datetime.now()
                    if proc.poll() is not None:
                        remaining = (planned_end - now).total_seconds()
                        if remaining > restart_threshold:
                            logging.warning(f"Процесс для {stream} завершился преждевременно. Перезапуск на оставшиеся {remaining:.0f} сек.")
                            try:
                                new_proc = self._start_recording_for_stream(stream, int(remaining))
                                self.active_processes[stream] = {
                                    "process": new_proc,
                                    "start_time": datetime.now(),
                                    "duration": int(remaining)
                                }
                            except Exception as e:
                                logging.error(f"Ошибка перезапуска записи для {stream}: {e}")
                        else:
                            logging.info(f"Запись для {stream} завершена корректно.")
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
            logging.info("Обнаружено изменение основного конфигурационного файла, перезагружаем...")
            self.recorder.reload_config()
        # Если изменён конфиг go2rtc – перечитываем его и перестраиваем квоты
        elif event.src_path == str(self.recorder.go2rtc_config_path.resolve()):
            logging.info("Обнаружено изменение go2rtc-конфигурационного файла, перезагружаем...")
            try:
                new_go2rtc_config = self.recorder.load_go2rtc_config()
                self.recorder.go2rtc_config = new_go2rtc_config
                self.recorder.camera_quotas_gb = self.recorder._build_camera_quotas(new_go2rtc_config)
                logging.info("Конфигурация go2rtc успешно перезагружена и квоты обновлены.")
            except Exception as e:
                logging.error(f"Ошибка перезагрузки go2rtc-конфигурации: {e}")

def start_config_observer(recorder: VideoRecorder) -> Observer:
    """
    Запускает наблюдателя watchdog для конфигурационного файла.

    :param recorder: Экземпляр VideoRecorder.
    :return: Объект Observer.
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

    :param recorder: Экземпляр VideoRecorder.
    :return: Объект Flask.
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
