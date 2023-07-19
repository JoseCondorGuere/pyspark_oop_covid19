# Ejecución y Prueba en Entorno Local Intellij 
Requerimientos:
- Tener Spark instalado y configurado, tomar como referente el siguiente link https://www.geeksforgeeks.org/install-apache-spark-in-a-standalone-mode-on-windows/
- Descargar IntelliJ IDEA Community Edition desde  https://www.jetbrains.com/idea/download/download-thanks.html?platform=windows&code=IIC

Se desarrolló en PySpark,

Primero, 
- Descomprimir el archivo *1. local* , luego abrir Intellij IDEA yen ella abriran el proyecto *covid19_spark* 

![Alt text](/img/image3.png)

- Modificar configuraciones de ejecución

![Alt text](/img/image-1.png)

- Indicar la ruta del archivo *main.py* archivo que contiene la clase principal de ejecución

![Alt text](/img/image-2.png)

- Estructura de archivos 

![Alt text](/img/image-3.png)

## Explicación de Módulos
- src                       / Carpeta principal de código y recursos funcionales
    - main.py               / Archivo principal de ejecución
    - jobs 
        - ingest.py         / Job de Lectura de datos y almacenarlo en dataframes para su reutilización en el proceso
        - dataClean.py      / Job de limpieza de datos, para el ejemplo se hizo limpieza de campos string
        - dataProcess.py    / Job de Escritura y validación Offset de Datos - Valida nuevos registros y los ingresa a la tabla actual
        
    - schema 
        - structureSchema.py / Archivo primordial de configuración de Schemas y Path de archivos csv de la carpeta data

- data                      / Carpeta donde se almacenan los archivos .csv
- output                    / Carpeta de escritura en parquet
- config                    / Carpeta de configuración de recursos de Spark
    - sparkSettings.json    / Json que contiene las configuraciones de spark, memoria, instancias, core


# Test de Clases y Ejecución Spark en Colab

Se modificó el código de ejecución local y se adaptó para realizar la prueba pertinente en Google Colab


Primero
- Acceder al siguiente link de Colab https://colab.research.google.com/
- Después de haber iniciado sesión ubicarse en la opción de *Subir Notebook*!

![Alt ts](/img/image.png)

- Importar el notebook ubicado en el comprimido carpeta *2. colab*

![Alt text](/img/image2.png)

Segundo, es necesario en crear la carpeta "data"

![imimg.png](/img/img.png)

Tercero, ejecutar todas las celdas

![img_1.png](/img/img_1.png)

Finalmente validar la salida en la carpeta "output/" en formato parquet



