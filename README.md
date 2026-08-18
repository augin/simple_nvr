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
- Архив обрезанных клипов с удалением
- Управление записью (старт/стоп) из интерфейса
- Система тревог (TCP-сервер Dahua/XM, HTTP Hikvision)
- Системные логи с кольцевым буфером и автопрокруткой
- Авторизация с ролями admin/user
- Управление пользователями через веб-интерфейс
- Режим киоска — отдельный HTTP без авторизации с ролью user
- Светлая и тёмная темы
- Docker-образ
- Восстановление повреждённых MP4 (отсутствующий moov-атом) через ffmpeg

### Управление камерами через go2rtc

- Просмотр статуса go2rtc (работает/версия/обновление)
- Добавление и удаление камер (RTSP, DVR-IP/XMEye, ONVIF, ISAPI)
- Перетаскивание камер для изменения порядка в go2rtc.yaml
- Лимит хранения по каждой камере (ГБ)
- Проверка обновлений go2rtc через GitHub API
- Ручное обновление go2rtc с прогресс-баром
- Автоматическая установка go2rtc при первом запуске (systemd + конфиг)

## Требования

- **go2rtc** — запущенный сервер с RTSP-потоками (устанавливается автоматически при первом запуске или вручную)
- **ffmpeg** — для записи и обрезки видео
- **Go 1.25+** — для сборки

## Установка

### APT (Debian/Ubuntu)

```bash
curl -fsSL https://deb.augin.ru/signing-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/augin.gpg
echo "deb [signed-by=/usr/share/keyrings/augin.gpg] https://deb.augin.ru/ stable main" | sudo tee /etc/apt/sources.list.d/augin.list
sudo apt update
sudo apt install simple-nvr
```

Пакет устанавливает:
- Бинарник: `/usr/bin/simple-nvr`
- Конфиг: `/etc/simple-nvr/nvr.yaml`
- Статика: `/usr/share/simple-nvr/`
- Данные: `/var/lib/simple-nvr/`

### Сборка из исходников

```bash
go build -o nvr .
```

### Структура проекта

```
main.go            — точка входа, HTTP-маршруты, планировщик
config.go          — загрузка/сохранение конфигурации (NVR, go2rtc)
auth.go            — авторизация, сессии, роли
recorder.go        — запись RTSP-потоков через ffmpeg
storage.go         — управление диском, лимиты, очистка
alarm.go           — TCP-сервер тревог Dahua/XM, MQTT
alarm_hikvision.go — HTTP-сервер тревог Hikvision
alarm_common.go    — общие функции и константы тревог
recovery.go        — восстановление повреждённых MP4
nal_parser.go      — парсинг NAL-单元 H.264/HEVC, SPS/PPS/VPS
logs.go            — кольцевой буфер системных логов
kiosk.go           — режим киоска (reverse proxy)
api.go             — ядро API, структура API
api_cameras.go     — CRUD камер, go2rtc reorder
api_files.go       — просмотр записей, скачивание, обрезка
api_archive.go     — архив обрезанных клипов
api_config.go      — настройки, статус, управление записью
api_alarm.go       — API тревог (Dahua + Hikvision)
api_go2rtc.go      — установка/обновление/restart go2rtc
api_tools.go       — сканер записей, восстановление
api_users.go       — управление пользователями
api_logs.go        — API системных логов
```

### Конфигурация

Создайте файл `/etc/simple-nvr/nvr.yaml`:

```yaml
base_dir: '/var/lib/simple-nvr/recordings'
archive_dir: '/var/lib/simple-nvr/archive'
stream_server: 'rtsp://127.0.0.1:8554'
default_camera_limit_gb: 90
global_size_gb: 500
go2rtc_config_path: /etc/go2rtc/go2rtc.yaml
http_port: 8180
kiosk_enabled: false
kiosk_port: 8181
```

### Запуск

```bash
simple-nvr --config /etc/simple-nvr/nvr.yaml --static-dir /usr/share/simple-nvr
```

При запуске без флагов бинарник автоматически ищет конфиг в `/etc/simple-nvr/nvr.yaml`, а статику рядом с собой.

### Systemd-сервис

При установке через APT сервис создаётся автоматически. Для ручной настройки:

```ini
[Unit]
Description=Simple NVR
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/bin/simple-nvr --config /etc/simple-nvr/nvr.yaml --static-dir /usr/share/simple-nvr
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
  -p 8181:8181 \
  -v /path/to/config:/etc/simple-nvr \
  -v /path/to/video:/var/lib/simple-nvr/recordings \
  -v /path/to/go2rtc:/etc/go2rtc \
  simple-nvr
```

В Docker-образе киоск-режим включён по умолчанию (порт 8181). Чтобы отключить, добавьте `kiosk_enabled: false` в конфиг.

## Авторизация

Если в `/etc/simple-nvr/users.yaml` нет пользователей, авторизация отключена — все вкладки доступны без логина.

При создании первого пользователя он автоматически становится администратором.

### Роли

| Роль | Доступ |
|------|--------|
| **admin** | Все вкладки, управление записью, настройки, управление пользователями, удаление архива, добавление/удаление камер, перемещение камер |
| **user** | Камеры, архив (просмотр, обрезка, скачивание) |

### Управление пользователями

В вкладке **Настройки → Пользователи**:
- Добавление пользователей (логин, пароль, роль)
- Смена пароля
- Удаление пользователей

Пароли хранятся в bcrypt-hashed виде.

## Режим киоска

Режим киоска запускает отдельный HTTP-сервер (по умолчанию порт 8181), который проксирует API на основной сервер с ролью `user`. Видны только вкладки **Камеры** и **Архив** — все admin-вкладки скрыты.

Включение:
- **UI**: Настройки → Пользователи → toggle «Режим киоска»
- **Конфиг**: `kiosk_enabled: true`, `kiosk_port: 8181`

В Docker-образе киоск включён по умолчанию.

## Системные логи

Вкладка **Логи** показывает все системные сообщения в реальном времени:
- Кольцевой буфер на 1000 записей
- Автопрокрутка к новым записям
- Цветовая кодировка по уровню (INFO/WARN/ERROR/DEBUG)
- Очистка буфера

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

### Публичные (без авторизации)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/version` | Версия приложения |
| GET | `/api/auth/check` | Проверка необходимости авторизации |

### Авторизация

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| POST | `/api/auth/login` | Публичный | Вход (username, password) |
| POST | `/api/auth/logout` | Авторизованные | Выход |
| GET | `/api/auth/me` | Авторизованные | Текущий пользователь |

### Камеры и записи

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/api/cameras` | Авторизованные | Список камер из go2rtc |
| GET | `/api/files?camera=X` | Авторизованные | Записи камеры по датам |
| GET | `/api/video/<camera>/<Y>/<M>/<D>/<file>` | Авторизованные | Просмотр записи |
| GET | `/api/download?camera=&folder=&file=&start=&end=` | Авторизованные | Обрезка и скачивание |
| GET | `/api/status` | Авторизованные | Статус записи и хранилища |
| GET | `/api/storage/cameras` | Авторизованные | Хранилище по камерам |

### Архив

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/api/archive?camera=X` | Авторизованные | Файлы архива камеры |
| GET | `/api/archive/video/<camera>/<Y>/<M>/<D>/<file>` | Авторизованные | Просмотр архива |
| POST | `/api/archive/delete?camera=&folder=&file=` | Admin | Удаление из архива |

### Управление

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| POST | `/api/tools/repair` | Admin | Восстановление повреждённого MP4 (body: {path}) |
| POST | `/api/record/start` | Admin | Начать запись |
| POST | `/api/record/stop` | Admin | Остановить запись |
| GET | `/api/config` | Авторизованные | Текущая конфигурация |
| POST | `/api/config` | Admin | Сохранить конфигурацию |

### Тревоги

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/api/alarm/status` | Авторизованные | Статус alarm-серверов (Dahua/XM и Hikvision) |
| POST | `/api/alarm/start` | Admin | Запуск Dahua/XM alarm-сервера |
| POST | `/api/alarm/stop` | Admin | Остановка Dahua/XM alarm-сервера |
| POST | `/api/hikvision/start` | Admin | Запуск Hikvision alarm-сервера |
| POST | `/api/hikvision/stop` | Admin | Остановка Hikvision alarm-сервера |
| GET | `/api/alarm/log` | Авторизованные | Журнал событий |
| POST | `/api/alarm/clear` | Admin | Очистка журнала |
| GET | `/api/alarms/range?since=&until=` | Авторизованные | События за период |

### Go2rtc и камеры

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/api/go2rtc/status` | Admin | Статус go2rtc (running, версия, обновление, install_needed) |
| POST | `/api/go2rtc/restart` | Admin | Перезапуск go2rtc |
| POST | `/api/go2rtc/update` | Admin | Обновление go2rtc (body: {url}) |
| POST | `/api/go2rtc/install` | Admin | Установка go2rtc (body: {url}), создаёт systemd-unit и пустой конфиг |
| GET | `/api/go2rtc/cameras` | Admin | Список камер из go2rtc.yaml (name, type, ip, limit_gb) |
| POST | `/api/go2rtc/cameras` | Admin | Добавление камеры (name, type, user, pass, ip, port, channel, limit_gb) |
| DELETE | `/api/go2rtc/cameras?name=X` | Admin | Удаление камеры |
| POST | `/api/go2rtc/reorder` | Admin | Перемещение камеры (body: {from, to}) |

### Логи и пользователи

| Метод | Путь | Доступ | Описание |
|-------|------|--------|----------|
| GET | `/api/logs` | Авторизованные | Системные логи |
| POST | `/api/logs/clear` | Admin | Очистка логов |
| GET | `/api/users` | Admin | Список пользователей |
| POST | `/api/users` | Admin* | Создание пользователя |
| DELETE | `/api/users?username=X` | Admin | Удаление пользователя |
| POST | `/api/users/change-password` | Admin/User | Смена пароля |

*Первый пользователь создаётся без авторизации.

## Веб-интерфейс

Откройте `http://<host>:8180` в браузере.

### Вкладки

- **Камеры** — просмотр записей, обрезка, скачивание, иконки тревог на записях
- **Архив** — обрезанные клипы, удаление (только admin)
- **Мониторинг** — активные процессы записи, статистика хранилища (admin)
- **Тревоги** — alarm-серверы (Dahua/XM, Hikvision), журнал событий (admin)
- **Логи** — системные логи с автопрокруткой (admin)
- **Настройки** — основные настройки, лимиты по камерам, управление камерами/go2rtc, управление пользователями (admin)
- **Сканер** — проверка записей на повреждения и восстановление через ffmpeg (только admin)

### Подвкладки «Настройки»

- **Основные** — директории хранения, лимиты, порт HTTP
- **Лимиты по камерам** — редактирование лимитов хранения (ГБ) и дней для каждой камеры
- **Камеры / go2rtc** — настройки RTSP-сервера и go2rtc, добавление/удаление/перемещение камер, обновление и установка go2rtc
- **Пользователи** — управление учётными записями, режим киоска (только admin)

### Управление камерами

- Добавление: выбор типа (RTSP, DVR-IP, ONVIF, ISAPI), ввод IP/логина/пароля, установка лимита ГБ
- Удаление: кнопка «Удалить» рядом с каждой камерой
- Перемещение: drag-and-drop (перетаскивание за маркер `⠿`)
- Порядок камер сохраняется в `go2rtc.yaml` и применяется immediately

### Управление видео

- Скорость: 1x, 2x, 4x, 8x, 10x
- Зум: скролл мыши (до 10x)
- Панорамирование: drag при зуме
- Сброс зума: двойной клик

## Лицензия

MIT
