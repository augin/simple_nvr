# Используем базовый образ Alpine Linux
FROM alpine:latest

# Установка Nginx
RUN apk add --no-cache nginx

# Создание директории для PID файла Nginx
RUN mkdir -p /run/nginx

# Установка PHP и основных расширений
RUN apk add --no-cache php8 php8-fpm php8-mbstring php8-json php8-yaml

# Установка Python
RUN apk add --no-cache python3 python3-yaml

# Копирование конфигурации Nginx в контейнер
COPY nginx.conf /etc/nginx/nginx.conf

# Копирование скрипта запуска
COPY nvr.py /opt/simple_nvr/nvr.py
COPY nvr.yaml /config/nvr.yaml
COPY www/ /opt/simple_nvr/
# Открытие портов для Nginx
EXPOSE 80

VOLUME /config
WORKDIR /config

CMD ["python3", "nvr.py", "--config_file", "/config/nvr.yaml"]
