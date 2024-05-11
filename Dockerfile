# Используем базовый образ Alpine Linux
FROM alpine:latest

# Установка Nginx
RUN apk add --no-cache nginx

# Создание директории для PID файла Nginx
RUN mkdir -p /run/nginx

# Установка PHP и основных расширений
RUN apk add --no-cache php83 php83-fpm php83-mbstring php83-json php83-pecl-yaml

# Установка Python
RUN apk add --no-cache python3 py3-yaml py3-schedule

# Копирование конфигурации Nginx в контейнер
COPY nginx.conf /etc/nginx/nginx.conf

# Копирование скрипта запуска
RUN mkdir -p /opt/simple_nvr/
COPY nvr.py /opt/simple_nvr/nvr.py
COPY nvr.yaml /config/nvr.yaml
COPY www /opt/simple_nvr/www
# Открытие портов для Nginx
EXPOSE 8180

VOLUME /config
WORKDIR /config

CMD php-fpm83; nginx; python3 nvr.py --config_file /config/nvr.yaml
