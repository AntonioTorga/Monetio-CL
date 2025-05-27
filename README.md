# Monetio-CL
Versión inicial de un adaptador para Melodies-Monet de las redes SINCA y DMC. La interfaz de este software es vía línea de comandos.

## Idea de este proyecto
Crear un software que pueda descargar y procesar automáticamente la data obtenida de las redes SINCA y DMC.

## Estado actual
En este momento Monetio-CL procesa los datos de la red en formato "intermedio" (data tabulada en formato wide, guardado en formato .csv) al formato deseado para funcionar con Melodies-Monet.

## Instalación
Para instalar debe clonar este repositorio y ejecutar dentro de un ambiente virtual
´´´
pip install -e .
´´´
## Uso
La interfaz de comandos puede ser accedida desde la terminal escribiendo Monetio-CL.
Para obtener más información de su uso ejecute:
´´´
Monetio-CL --help
´´´
