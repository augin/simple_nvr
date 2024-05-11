import argparse
import time
import os
import yaml
from datetime import datetime
import subprocess
import schedule
from pathlib import Path


# Настройка парсера аргументов командной строки
parser = argparse.ArgumentParser(description='Запись видеопотоков.')
parser.add_argument('--config_file', type=str, help='Путь к файлу конфигурации', required=True)
# Чтение аргументов командной строки
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


# Функция очистки папок
def clean_camera_folders(base_dir, target_size_gb):
    """
    Очищает папки камер, удаляя самые старые файлы, пока размер папки не уменьшится до целевого размера.

    :param base_dir: Путь к каталогу, содержащему папки камер.
    :param target_size_gb: Целевой размер папки в гигабайтах.
    """
    base_dir = Path(base_dir)
    # Удалить пустые папки
    for folder in base_dir.iterdir():
        if folder.is_dir() and not any(folder.iterdir()):
            folder.rmdir()
    # Перебор папок камер
    for camera_dir in base_dir.iterdir():
        if camera_dir.is_dir():
            # Получить текущий размер папки камеры в ГБ без десятичной части
            size_gb = sum(f.stat().st_size for f in camera_dir.glob('**/*') if f.is_file()) / (1024 ** 3)

            # Рассчитать, сколько нужно удалить, чтобы достичь целевого размера
            space_to_free_gb = size_gb - target_size_gb

            # Проверить, превышает ли текущий размер целевой размер
            if size_gb > target_size_gb:
                print(f"Cleaning {camera_dir}")
                # Удалить самые старые файлы, пока размер папки не уменьшится до целевого размера
                while space_to_free_gb > 0:
                    # Найти самый старый файл в папке
                    oldest_file = min(camera_dir.glob('**/*'), key=os.path.getmtime)
                    # Удалить самый старый файл
                    oldest_file_size = oldest_file.stat().st_size / (1024 ** 3)
                    try:
                        oldest_file.unlink()
                        print(f"Deleted {oldest_file}")
                        # Обновить текущий размер
                        space_to_free_gb -= oldest_file_size
                    except Exception as e:
                        print(f"Error deleting file {oldest_file}: {e}")

# Функция для записи потоков
def record_streams(duration, base_dir, stream_server):
    with open(go2rtc_config_path, 'r') as file:
        config_data = yaml.safe_load(file)
    streams = config_data['streams'].keys()

    now = datetime.now()
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    current_time = now.strftime("%H-%M")
    M = now.strftime("%M")

    processes = []

    for stream_name in streams:
        directory = os.path.join(base_dir, stream_name, year, month, day)
        os.makedirs(directory, exist_ok=True)
        output_file = os.path.join(directory, f"{current_time}.mp4")
        command = [
            'ffmpeg', '-hide_banner', '-loglevel', 'warning', '-threads', '2',
            '-avoid_negative_ts', 'make_zero', '-fflags', '+nobuffer+genpts+discardcorrupt',
            '-flags', 'low_delay', '-rtsp_transport', 'tcp', '-use_wallclock_as_timestamps', '1',
            '-i', f"{stream_server}/{stream_name}", '-reset_timestamps', '1', '-strftime', '1',
            '-c:v', 'copy', '-c:a', 'aac', '-strict', 'experimental', '-t', str(duration), output_file
        ]
        #log_file = os.path.join(base_dir, f"{stream_name}_{M}.txt")
        # Запуск субпроцесса без ожидания его завершения
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        processes.append(process)

    # Возвращаем список запущенных процессов, если нужно контролировать их в будущем
    return processes
    # Очистка папок до размера 90 Гб
    clean_camera_folders(base_dir, target_size_gb)

## Запуск записи при старте.
now = datetime.now()
#if now.minute % 10 != 0:
next_interval = (now.minute // 10 + 1) * 10
remaining_time = (next_interval - now.minute) * 60 - now.second
record_streams(remaining_time, base_dir, stream_server)

duration = 607
# Планируем задачу на каждые 10 минут
schedule.every().hour.at(":00").do(record_streams, duration, base_dir, stream_server)
schedule.every().hour.at(":10").do(record_streams, duration, base_dir, stream_server)
schedule.every().hour.at(":20").do(record_streams, duration, base_dir, stream_server)
schedule.every().hour.at(":30").do(record_streams, duration, base_dir, stream_server)
schedule.every().hour.at(":40").do(record_streams, duration, base_dir, stream_server)
schedule.every().hour.at(":50").do(record_streams, duration, base_dir, stream_server)

# Основной цикл
while True:
    schedule.run_pending()
    time.sleep(1)