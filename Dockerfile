# imagen con la que se trabajara, esto es para agregar librerias al contenedor de docker/airflow
FROM apache/airflow:2.1.4
# directorio donde se va a trabajar, en este caso se trabaja en el mismo nivel donde esta el Dockerfile
WORKDIR ./
# primero el archivo que se va a copiar desde local y luego el directorio donde se va a acolocar en el contenedor
COPY requirements.txt ./
# instalar con pip
RUN pip install -r requirements.txt
#agregar archivos desde local hacia el contenedor
# ADD images /directorio/del/contenedor
# correr comandos en terminal dentro del contenedor
# ENTRYPOINT [ "python", "archivo.py" ]