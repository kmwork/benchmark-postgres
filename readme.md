Параметры запуска<br/>
-h=127.0.0.1 - точки подключения к Кассандре, может присутствовать несколько<br/>
-p=9042 - порт подключения<br/>
-l=postgres - логин авторизации подключения<br/>
-w=1 - пароль авторизации подключения<br/>
-k=demo - cхема <br/>
-s=1000 - размер пакета (SINGLE - размер пакета, который отсылается в базу за 1 раз, MULTI - количество столбцов под значения датчиков)<br/>
-n=10 - количество итераций генерации (0 - бесконечно)<br/>
-c=15000 - количество датчиков (только для SINGLE версии)<br/>
-r=false - требуется ли пересоздавать таблицы (true - таблица будет удалена и создана заново)<br/>
-m=SINGLE - режим (SINGLE: один датчик на строку, MULTI - много датчиков на строку)<br/>

нужно поставить реширения на посресс
CREATE EXTENSION hstore;