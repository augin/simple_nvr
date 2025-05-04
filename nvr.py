import argparse
import time
import os
import yaml
from datetime import datetime
import subprocess
import schedule
from pathlib import Path
import signal

# Глобальный словарь для хранения активных процессов
active_processes = {}

# Настройка парсера аргументов командной строки
parser = argparse.ArgumentParser(description='Запись видеопотоков.')
parser.add_argument('--config_file', type=str, help='Путь к файлу конфигурации', required=True)
args = parser.parse_args()
dvr_config_file = args.config_file

def read_dvr_config(config_file):
    with open(dvr_config_file, 'r') as file:
        return yaml.safe_load(file)

config = read_dvr_config(dvr_config_file)
base_dir = config['base_dir']
stream_server = config['stream_server']
target_size_gb = config['target_size_gb']
go2rtc_config_path = config['go2rtc_config_path']

def clean_camera_folders(base_dir, target_size_gb):
    """Очищает папки камер, удаляя самые старые файлы, а затем пустые директории."""
    base_dir = Path(base_dir)

    # 1. Удаляем старые файлы, если превышен лимит размера
    for camera_dir in base_dir.iterdir():
        if camera_dir.is_dir():
            size_gb = sum(f.stat().st_size for f in camera_dir.glob('**/*') if f.is_file()) / (1024 ** 3)
            space_to_free_gb = size_gb - target_size_gb

            if space_to_free_gb > 0:
                print(f"Cleaning {camera_dir}")
                files = sorted(camera_dir.glob('**/*'), key=os.path.getmtime)

                for file in files:
                    if space_to_free_gb <= 0:
                        break
                    if file.is_file():
                        try:
                            file_size_gb = file.stat().st_size / (1024 ** 3)
                            file.unlink()
                            print(f"Deleted {file}")
                            space_to_free_gb -= file_size_gb
                        except Exception as e:
                            print(f"Error deleting file {file}: {e}")

    # 2. Удаляем пустые папки (рекурсивно)
    for root, dirs, files in os.walk(base_dir, topdown=False):
        for dir_name in dirs:
            dir_path = Path(root) / dir_name
            try:
                if not any(dir_path.iterdir()):  # Если папка пуста
                    dir_path.rmdir()
                    print(f"Deleted empty directory: {dir_path}")
            except Exception as e:
                print(f"Error deleting directory {dir_path}: {e}")

def record_streams(duration, base_dir, stream_server):
    """Записывает потоки и возвращает словарь процессов."""
    with open(go2rtc_config_path, 'r') as file:
        config_data = yaml.safe_load(file)

    now = datetime.now()
    timestamp = now.strftime("%H-%M")
    date_path = os.path.join(now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"))

    processes = {}

    for stream_name in config_data['streams'].keys():
        directory = os.path.join(base_dir, stream_name, date_path)
        os.makedirs(directory, exist_ok=True)
        output_file = os.path.join(directory, f"{timestamp}.mp4")

        command = [
            'ffmpeg',
            '-hide_banner',
            '-loglevel', 'error',
            '-rtsp_transport', 'tcp',
            '-avoid_negative_ts', 'make_zero',
            '-fflags', '+nobuffer+genpts+discardcorrupt',
            '-flags', 'low_delay',
            '-use_wallclock_as_timestamps', '1',
            '-i', f"{stream_server}/{stream_name}",
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-t', str(duration),
            output_file
        ]

        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid
        )
        processes[stream_name] = process

    # Обновляем глобальный словарь процессов
    for name, process in processes.items():
        active_processes[name] = process

    clean_camera_folders(base_dir, target_size_gb)
    return processes

def cleanup_processes():
    """Завершает все активные процессы записи."""
    for name, process in list(active_processes.items()):
        if process.poll() is None:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                print(f"Завершен процесс записи для {name}")
            except ProcessLookupError:
                pass
        del active_processes[name]

def handle_termination(signum, frame):
    """Универсальный обработчик сигналов завершения"""
    signals = {
        signal.SIGINT: "SIGINT",
        signal.SIGTERM: "SIGTERM",
        signal.SIGHUP: "SIGHUP"
    }
    print(f"\nПолучен сигнал {signals.get(signum, str(signum))}, завершаю работы...")

    # 1. Отправляем SIGTERM всем процессам
    cleanup_processes()

    # 2. Даем процессам 5 секунд на корректное завершение
    time.sleep(5)

    # 3. Если процессы еще живы, отправляем SIGKILL
    for name, process in list(active_processes.items()):
        if process.poll() is None:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                print(f"Принудительно завершен процесс записи для {name}")
            except ProcessLookupError:
                pass

    schedule.clear()
    exit(0)

# Регистрируем обработчики для всех сигналов завершения
signal.signal(signal.SIGINT, handle_termination)
signal.signal(signal.SIGTERM, handle_termination)
signal.signal(signal.SIGHUP, handle_termination)  # Добавляем обработку SIGHUP

# Основные настройки
duration = 607  # 10 минут 7 секунд

# Планирование задач
for minute in ['00', '10', '20', '30', '40', '50']:
    schedule.every().hour.at(f":{minute}").do(record_streams, duration, base_dir, stream_server)

# Запуск записи при старте
now = datetime.now()
next_interval = (now.minute // 10 + 1) * 10
remaining_time = (next_interval - now.minute) * 60 - now.second
record_streams(remaining_time, base_dir, stream_server)

# Основной цикл
while True:
    schedule.run_pending()

    # Очищаем завершенные процессы
    for name, process in list(active_processes.items()):
        if process.poll() is not None:
            del active_processes[name]

    time.sleep(1)
