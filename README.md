# Анализ публикаций в Scopus с использованием Scopus API

## Структура проекта

* scopusCrawler/APIScopusCrawler - модуль API (выгружает данные с API, очищает и загружает в базу данных).
* scopusCrawler/WebScopusCrawler - модуль Web (выгружает данные с сайта SCOPUS, очищает и загружает в базу данных).

## Установка и запуск

**Требования для установки и запуска**: 
* docker
* docker-compose
* git

Описанные действия выполнялись на операционной системе Ubuntu 22.04
* Клонировать репозиторий: https://github.com/KorolevDenis/BIGDATA_2022
* Перейти в директорию проекта: cd ~/BIGDATA_2022/scopusCrawler
* Установить зависимости из файла requirements.txt
* Для запуска API модуля:
  * Перейти в директорию: cd ~/BIGDATA_2022/scopusCrawler/APIScopusCrawler
  * Выполнить: scrapyd-deploy default
  * Запустить скрипт: python api_run.py
    Этот скрипт запускает scrapy процессы для каждого года из промежутка, переданного параметрами. Принимает на вход 3 параметра:
    * левая граница промежутка годов (включая левую границу)
    * правая граница промежутка годов (не включая праваю границу)
    * количество потоков, на которых будет запущен API модуль
	
* Для запуска Web модуля:
  * Перейти в директорию: cd ~/BIGDATA_2022/scopusCrawler/WebScopusCrawler
  * Запустить скрипт: python web_run.py
    Этот скрипт запускает scrapy процессы для каждого года из промежутка, переданного параметрами. Принимает на вход 4 параметра:
    * левая граница промежутка годов (включая левую границу)
    * правая граница промежутка годов (не включая праваю границу)
    * количество потоков, на которых будет запущен Web модуль

Основные переменные окружения доступны в файле .env. Grafana доступна по адресу http://localhost:3000.