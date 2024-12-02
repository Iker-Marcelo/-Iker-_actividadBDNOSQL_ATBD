El script utiliza la siguiente cadena de conexión: mongodb://root:root@localhost:27017/
Para utilizar esa cadena de conexión, debe haber un usuario en MongoDB con las credenciales usuario: root y contraseña: root

Puede ser necesario instalar pandas y pymongo. Se puede usar el siguiente comando en la terminal:
pip install pandas pymongo

Para ejecutar el script, poner en la terminal el comando: python create_collections.py

Al ejecutar el script, borrará la colección players si existe en la base de datos women-football. Creará la colección con esquema, se insertarán 100 registros del csv, se actualizarán los 2 primeros nombres a mayúsculas y se calculará el tiempo de las consultas.
Después se mostrará un menú interactivo para elegir que consulta ejecutar y se mostrará el resultado por consola.
