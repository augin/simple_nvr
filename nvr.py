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
from typing import Dict, Any

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def read_dvr_config(config_file: str) -> Dict[str, Any]:
    """
    Читает конфигурационный YAML-файл и возвращает словарь настроек.
    
    :param config_file: Путь к YAML-файлу конфигурации.
    :return: Словарь настроек.
    """
    try:
        with open(config_file, 'r') as file:
            config_data = yaml.safe_load(file)
        return config_data
    except Exception as e:
        logging.error(f"Ошибка чтения конфигурационного файла {config_file}: {e}")
        raise


class VideoRecorder:
    """
    Класс для записи видеопотоков, управления процессами и очистки пространства.
    """
    def __init__(self, dvr_config_file: str) -> None:
        self.config = read_dvr_config(dvr_config_file)
        self.base_dir: Path = Path(self.config['base_dir'])
        self.stream_server: str = self.config['stream_server']
        self.target_size_gb: float = self.config['target_size_gb']
        self.go2rtc_config_path: Path = Path(self.config['go2rtc_config_path'])
        self.active_processes: Dict[str, subprocess.Popen] = {}

        # Кэширование конфигурации go2rtc
        self.go2rtc_config = self.load_go2rtc_config()

    def load_go2rtc_config(self) -> Dict[str, Any]:
        """
        Загружает и кэширует конфигурацию go2rtc.
        
        :return: Словарь с настройками потоков.
        """
        try:
            with self.go2rtc_config_path.open('r') as file:
                config_data = yaml.safe_load(file)
            return config_data
        except Exception as e:
            logging.error(f"Ошибка чтения go2rtc-конфигурации {self.go2rtc_config_path}: {e}")
            raise

    def clean_camera_folders(self, min_age_seconds: int = 3600) -> None:
        """
        Удаляет старые файлы и пустые директории. Каталоги, созданные менее min_age_seconds назад, не трогаются.
    
        :param min_age_seconds: Минимальный возраст каталога (в секундах) для его удаления, если он пустой.
        """
        now = time.time()

        # 1. Удаляем старые файлы, если размер превышает целевой (в ГБ)
        for camera_dir in self.base_dir.iterdir():
            if camera_dir.is_dir():
                try:
                    size_bytes = sum(f.stat().st_size for f in camera_dir.glob('**/*') if f.is_file())
                    size_gb = size_bytes / (1024 ** 3)
                except Exception as e:
                    logging.error(f"Ошибка при расчете размера каталога {camera_dir}: {e}")
                    continue

                space_to_free_gb = size_gb - self.target_size_gb
                if space_to_free_gb > 0:
                    logging.info(f"Очистка каталога {camera_dir} — нужно освободить {space_to_free_gb:.3f} ГБ.")
                    try:
                        files = sorted(camera_dir.glob('**/*'), key=lambda f: f.stat().st_mtime)
                    except Exception as e:
                        logging.error(f"Ошибка сортировки файлов в {camera_dir}: {e}")
                        continue

                    for file in files:
                        if space_to_free_gb <= 0:
                            break
                        if file.is_file():
                            try:
                                file_size_gb = file.stat().st_size / (1024 ** 3)
                                file.unlink()
                                logging.info(f"Удален файл {file} размером {file_size_gb:.3f} ГБ.")
                                space_to_free_gb -= file_size_gb
                            except Exception as e:
                                logging.error(f"Ошибка при удалении файла {file}: {e}")

        # 2. Удаляем пустые папки (рекурсивно), за исключением недавно созданных
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

    def record_streams(self, duration: int) -> Dict[str, subprocess.Popen]:
        """
        Записывает потоки в течение указанного времени и возвращает словарь запущенных процессов.

        :param duration: Длительность записи в секундах.
        :return: Словарь в формате {имя_потока: процесс}.
        """
        now = datetime.now()
        timestamp = now.strftime("%H-%M")
        date_path = Path(now.strftime("%Y")) / now.strftime("%m") / now.strftime("%d")

        processes: Dict[str, subprocess.Popen] = {}
        for stream_name in self.go2rtc_config.get('streams', {}):
            try:
                directory = self.base_dir / stream_name / date_path
                directory.mkdir(parents=True, exist_ok=True)
                output_file = directory / f"{timestamp}.mp4"

                command = [
                    'ffmpeg',
                    '-hide_banner',
                    '-loglevel', 'error',
                    '-rtsp_transport', 'tcp',
                    '-avoid_negative_ts', 'make_zero',
                    '-fflags', '+nobuffer+genpts+discardcorrupt',
                    '-flags', 'low_delay',
                    '-use_wallclock_as_timestamps', '1',
                    '-i', f"{self.stream_server}/{stream_name}",
                    '-c:v', 'copy',
                    '-c:a', 'aac',
                    '-t', str(duration),
                    str(output_file)
                ]

                process = subprocess.Popen(
                    command,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    preexec_fn=os.setsid
                )
                processes[stream_name] = process
                self.active_processes[stream_name] = process
                logging.info(f"Начата запись для {stream_name} в файл {output_file}.")
            except Exception as e:
                logging.error(f"Ошибка при запуске записи для {stream_name}: {e}")

        # После запуска записи производим очистку старых файлов и пустых папок
        self.clean_camera_folders()
        return processes

    def cleanup_processes(self) -> None:
        """
        Завершает все активные процессы записи, посылая SIGTERM, а затем при необходимости SIGKILL.
        """
        for name, process in list(self.active_processes.items()):
            if process.poll() is None:
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    logging.info(f"Завершен процесс записи для {name}.")
                except ProcessLookupError:
                    logging.warning(f"Процесс {name} уже завершен.")
                except Exception as e:
                    logging.error(f"Ошибка при завершении процесса {name}: {e}")
            self.active_processes.pop(name, None)

    def handle_termination(self, signum: int, frame) -> None:
        """
        Обрабатывает сигналы завершения работы, корректно завершая процессы.
        
        :param signum: Номер сигнала.
        :param frame: Текущий стек вызовов.
        """
        signal_names = {signal.SIGINT: "SIGINT", signal.SIGTERM: "SIGTERM", signal.SIGHUP: "SIGHUP"}
        logging.info(f"Получен сигнал {signal_names.get(signum, str(signum))}, завершаю работу.")

        # 1. Корректное завершение запущенных процессов
        self.cleanup_processes()

        # 2. Даём процессам время на корректное завершение
        time.sleep(5)

        # 3. Если какие-то процессы ещё живы — принудительное завершение
        for name, process in list(self.active_processes.items()):
            if process.poll() is None:
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                    logging.info(f"Принудительно завершен процесс записи для {name}.")
                except ProcessLookupError:
                    logging.warning(f"Процесс {name} уже завершен при принудительном завершении.")
                except Exception as e:
                    logging.error(f"Ошибка при принудительном завершении процесса {name}: {e}")
            self.active_processes.pop(name, None)

        schedule.clear()
        exit(0)


def main() -> None:
    parser = argparse.ArgumentParser(description='Запись видеопотоков.')
    parser.add_argument('--config_file', type=str, help='Путь к файлу конфигурации', required=True)
    args = parser.parse_args()
    dvr_config_file = args.config_file

    recorder = VideoRecorder(dvr_config_file)

    # Назначим обработчик сигналов завершения работы
    def termination_handler(signum, frame):
        recorder.handle_termination(signum, frame)

    signal.signal(signal.SIGINT, termination_handler)
    signal.signal(signal.SIGTERM, termination_handler)
    signal.signal(signal.SIGHUP, termination_handler)

    # Задаем длительность записи: 607 секунд (10 минут 7 секунд)
    duration = 607

    # Планирование записи: каждые 10 минут в начале интервалов (00, 10, 20, 30, 40, 50 минуты часа)
    for minute in ['00', '10', '20', '30', '40', '50']:
        schedule.every().hour.at(f":{minute}").do(recorder.record_streams, duration)

    # Расчет оставшегося времени до следующего 10-минутного интервала
    now = datetime.now()
    next_minute = ((now.minute // 10 + 1) * 10) % 60
    if next_minute <= now.minute:
        next_run = datetime(now.year, now.month, now.day, now.hour, next_minute) + timedelta(hours=1)
    else:
        next_run = datetime(now.year, now.month, now.day, now.hour, next_minute)
    remaining_time = (next_run - now).total_seconds()

    logging.info(f"Запуск начальной записи на оставшиеся {remaining_time:.0f} секунд до следующего интервала.")
    recorder.record_streams(int(remaining_time))

    # Основной цикл выполнения
    try:
        while True:
            schedule.run_pending()
            # Убираем завершенные процессы из словаря
            for name, process in list(recorder.active_processes.items()):
                if process.poll() is not None:
                    recorder.active_processes.pop(name, None)
            time.sleep(1)
    except Exception as e:
        logging.error(f"Ошибка в основном цикле: {e}")
        recorder.handle_termination(signal.SIGTERM, None)


if __name__ == '__main__':
    main()
