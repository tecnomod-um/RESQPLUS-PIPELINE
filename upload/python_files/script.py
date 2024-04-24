import pandas as pd
import subprocess
import os
import argparse

# Carga el archivo CSV
#df = pd.read_csv('/Users/catimc/Documents/PROYECTOS/RES-Q+/TRANSFORMATION/data/data.csv')
# Setup argparse para manejar argumentos de la línea de comandos
parser = argparse.ArgumentParser(description='Procesar datos CSV y generar TTL.')
parser.add_argument('csv_path', type=str, help='Ruta al archivo CSV a procesar')
args = parser.parse_args()

# Carga el archivo CSV especificado en la línea de comandos
df = pd.read_csv(args.csv_path)
# Agrupa las filas por 'field_id'
grouped = df.groupby('field_id')

# Directorio de trabajo actual y subdirectorios necesarios
current_directory = os.getcwd()
#rdf_dir = os.path.join(current_directory, 'rdf')
#instances_dir = os.path.join(rdf_dir, 'instances')
#rules_dir = os.path.join(rdf_dir, 'reglas')
#csv_dir = os.path.join(rdf_dir, 'csv')

# Crear los directorios si no existen
#os.makedirs(instances_dir, exist_ok=True)
#os.makedirs(rules_dir, exist_ok=True)
#os.makedirs(csv_dir, exist_ok=True)

for name, group in grouped:
    # Guarda el grupo actual en un archivo CSV en la carpeta csv
    #group_csv_filename = os.path.join(current_directory, f"{name}.csv")
    #group.to_csv(group_csv_filename, index=False)
    # Comprueba si 'pattern_type' está vacío en todo el grupo
    if group['pattern_type'].notna().any():  # Verifica que hay al menos un no vacío
        # Guarda el grupo actual en un archivo CSV en la carpeta csv
        #group_csv_filename = os.path.join(current_directory, f"{name}.csv")
        group_csv_filename = os.path.join('../csv/', f"{name}.csv")
        group.to_csv(group_csv_filename, index=False)
    
        # Ejecutar el script Python para generar las reglas
        #rules_filename = os.path.join(current_directory, f"{name}_reglasgenericas.yarrrml")
        rules_filename = os.path.join('../rules/', f"{name}_reglasgenericas.yarrrml")
        subprocess.run(['python3', 'generateRules.py', '--input', group_csv_filename, '--output', rules_filename], check=True)

        # Convertir YARRRML a TTL
        #ttl_output_path = os.path.join(current_directory, f"{name}_output-intermediate.ttl")
        ttl_output_path = os.path.join('../rml/', f"{name}_output-intermediate.ttl")
        #print(f"-i {rules_filename} -o {ttl_output_path}")
        subprocess.run(['yarrrml-parser', '-i', rules_filename, '-o', ttl_output_path], check=True)

        # Ejecutar el contenedor Docker para transformar TTL en TTL final
        
        final_ttl_output_path = os.path.join(current_directory, f"{name}_instancias-salida.ttl")
        #print(f"-m {ttl_output_path} -o {final_ttl_output_path}")
        #subprocess.run(['docker', 'run', '--platform', 'linux/amd64', '--rm', '-v', f"{current_directory}:/data", 'rmlio/rmlmapper-java', '-m', ttl_output_path, '-o', final_ttl_output_path], check=True)
       # subprocess.run([
        #'docker', 'run', '--platform', 'linux/amd64', '--rm',
        #'-v', f"{current_directory}:/data",
        #'rmlio/rmlmapper-java', '-m', f"/data/{name}_output-intermediate.ttl", 
        #'-o', f"/data/{name}_salida.ttl"
    #], check=True)
     
        subprocess.run([
        'docker', 'run', '--platform', 'linux/amd64', 
        '-v', "/Users/catimc/Documents/PROYECTOS/RES-Q+/upload/:/data",
        'rmlio/rmlmapper-java', '-m', '/Users/catimc/Documents/PROYECTOS/RES-Q+/upload/rml/'f"{name}_output-intermediate.ttl", 
        '-o', '/Users/catimc/Documents/PROYECTOS/RES-Q+/upload/instances/'f"{name}_salida.ttl"
    ], check=True)
    else:
        print(f"No se creó CSV para {name} porque 'pattern_type' está vacío en todo el grupo.")

# Concatena todos los archivos TTL en uno solo al final del proceso
#final_ttl_path = os.path.join(current_directory, 'final_output.ttl')
# Suponiendo que 'current_directory' es el directorio donde están los archivos TTL
with open(os.path.join(current_directory, 'final_output.ttl'), 'w') as outfile:
    for ttl_file in os.listdir(current_directory):
        if 'salida' in ttl_file:  # Chequea si 'salida' está en el nombre del archivo
            ttl_path = os.path.join(current_directory, ttl_file)
            with open(ttl_path, 'r') as infile:
                outfile.write(infile.read())
                outfile.write('\n')  # Asegúrate de añadir una nueva línea para separar los contenidos


print("Procesamiento completado para todos los field_id.")
