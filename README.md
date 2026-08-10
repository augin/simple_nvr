# Simple NVR

Лёгкий видеорегистратор на Go. Записывает RTSP-потоки через go2rtc, хранит записи по 10 минут, автоматически очищает старые файлы при превышении лимита диска.

## Возможности

- Запись нескольких RTSP-камер параллельно
- Автоматическое разделение на 10-минутные сегменты
- Расписание записи — выравнивание на границы 10-минутных интервалов
- Просмотр записей с выбором камеры и даты
- Управление скоростью воспроизведения (1x–10x)
- Зум скролом мыши с панорамированием
- Обрезка видео с выбором реального времени начала/конца
- Архив обрезанных клипов с管理和 удалением
- Управление записью (старт/стоп) из интерфейса
- Настройки через веб-интерфейс
- Светлая и тёмная темы
- Docker-образ
- Graceful shutdown (SIGTERM → SIGKILL)

## Требования

- **go2rtc** — запущенный сервер с RTSP-потоками (не входит в состав)
- **ffmpeg** — для записи и обрезки видео
- **Go 1.22+** — для сборки

## Установка

### Сборка

```bash
go build -o nvr .
```

### Конфигурация

Создайте файл `nvr.yaml`:

```yaml
base_dir: '/mnt/video'           # Директория хранения записей
archive_dir: '/mnt/archive'       # Директория архива (по умолчанию: base_dir/archive)
stream_server: 'rtsp://127.0.0.1:8554'  # Адрес go2rtc
target_size_gb: 90                # Лимит диска (ГБ)
go2rtc_config_path: /opt/go2rtc/go2rtc.yaml  # Путь к конфигу go2rtc
http_port: 8180                   # Порт веб-интерфейса (по умолчанию 8180)
```

### Запуск

```bash
./nvr --config nvr.yaml
```

### Systemd-сервис

```ini
[Unit]
Description=Simple NVR
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/simple_nvr
ExecStart=/opt/simple_nvr/nvr --config /opt/simple_nvr/nvr.yaml
Restart=on-failure
RestartSec=5
TimeoutStopSec=20
KillMode=mixed

[Install]
WantedBy=multi-user.target
```

### Docker

```bash
docker build -t simple-nvr .
docker run -d \
  --name simple-nvr \
  -p 8180:8180 \
  -v /path/to/config:/config \
  -v /path/to/video:/mnt/video \
  -v /path/to/go2rtc:/opt/go2rtc \
  simple-nvr
```

## Структура записей

```
<base_dir>/
  <camera_name>/
    2026/
      08/
        10/
          09-38.mp4    # Сегмент начался в 09:38
          09-48.mp4    # Сегмент начался в 09:48
```

## Архив

Обрезанные клипы сохраняются в `<archive_dir>/<camera>/YYYY/MM/DD/`:

```
<archive_dir>/
  <camera_name>/
    2026/
      08/
        10/
          09-38_000005-000015.mp4
```

## API

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/cameras` | Список камер из go2rtc |
| GET | `/api/files?camera=X` | Записи камеры по датам |
| GET | `/api/video/<camera>/<Y>/<M>/<D>/<file>` | Просмотр записи |
| GET | `/api/download?camera=&folder=&file=&start=&end=` | Обрезка и скачивание (секунды) |
| GET | `/api/archive?camera=X` | Файлы архива камеры |
| GET | `/api/archive/video/<camera>/<Y>/<M>/<D>/<file>` | Просмотр архива |
| POST | `/api/archive/delete?camera=&folder=&file=` | Удаление из архива |
| GET | `/api/status` | Статус записи и хранилища |
| POST | `/api/record/start` | Начать запись |
| POST | `/api/record/stop` | Остановить запись |
| GET | `/api/config` | Текущая конфигурация |
| POST | `/api/config` | Сохранить конфигурацию |

## Веб-интерфейс

Откройте `http://<host>:8180` в браузере.

### Вкладки

- **Камеры** — просмотр записей, обрезка, скачивание
- **Архив** — обрезанные клипы, удаление
- **Мониторинг** — активные процессы записи, статистика хранилища
- **Настройки** — путь к записям, архиву, RTSP-серверу, лимит диска

### Управление видео

- Скорость: 1x, 2x, 4x, 8x, 10x
- Зум: скролл мыши (до 10x)
- Панорамирование: drag при зуме
- Сброс зума: двойной клик

## Лицензия

MIT
