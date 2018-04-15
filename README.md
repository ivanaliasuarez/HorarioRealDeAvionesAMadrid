# HorarioRealDeAvionesAMadrid
Extracción del horario real de llegada de los aviones a Madrid de la página de Aena

## Requisitos de ejecución del programa
### Instalación de los siguientes paquetes
- selenium
- pandas
- csv
- urllib
- lxml
- re
- pathlib
- datetime
- time
- sys

### Instalación de chromedriver.exe
- Será necesario tener instalado chromedriver.exe
- En la función main del programa, será necesario actualizar el path al path the instalación de cada usuario
  - driver = webdriver.Chrome('path to chromedriver')
  
## Ejecución del programa
El programa se ejecutará de la siguiente manera:
python PEC1-Webcrawler.py

## Archivo de salida del programa
El programa crea un archivo de salida csv llamado - informaciondevuelosacumulado.csv

Este archivo es creado en el mismo directorio de donde se ejecuta el programa.

En caso de que el archivo exista, este será reutilizado para añadir más información de los datos de los vuelos.

El archivo está formado por cuatro columnas:
- Día de llegada del vuelo
- Número del vuelo
- Hora estimada de llegada
- Horal real de llegada
