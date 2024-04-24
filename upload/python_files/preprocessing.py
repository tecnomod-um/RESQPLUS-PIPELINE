import csv
import argparse

def main(csvdata, csvmapping, output_path):
    # Ruta al nuevo archivo CSV
    mapping_output_path = output_path
    # El encabezado específico como string, que luego convertiremos en lista
    encabezado_especifico_str = "field_id,pattern_type,source_procedure,procedure_part,procedure_result,ontology_mapping,observable,finding,procedure,OBLIGATORY,value_type,categorical_value,categorical_ontology_mapping,procedure_reason,procedure_location,measurement_unit,procedure_frequency,temporal_context,statement_context,case_id,field_value"
    encabezado_especifico = encabezado_especifico_str.split(',')

    data_csvdata = {}
    result_data_file = f'{mapping_output_path}/preprocessed_data.csv'
    with open(result_data_file, mode='w', newline='', encoding='utf-8-sig') as file_output:
        writer = csv.DictWriter(file_output, fieldnames=encabezado_especifico)
        writer.writeheader()

        with open(csvdata, mode='r', encoding='utf-8-sig') as file_data:
            reader_data = csv.DictReader(file_data)
            for row in reader_data:
                for header, value in row.items():
                    data_csvdata[header] = value

                with open(csvmapping, mode='r', encoding='utf-8-sig') as file_mapping:
                    reader_mapping = csv.DictReader(file_mapping)
                    for row in reader_mapping:
                        field_id_value = row['field_id'].strip()
                        if not field_id_value:
                            continue

                        if field_id_value in data_csvdata and data_csvdata[field_id_value] != '':
                            caseId = data_csvdata['case_id']
                            row['case_id'] = caseId
                            row['field_value'] = data_csvdata[field_id_value]
                            writer.writerow(row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process CSV data and mapping files.")
    parser.add_argument('csvdata', type=str, help='Path to the CSV data file')
    parser.add_argument('csvmapping', type=str, help='Path to the CSV mapping file')
    parser.add_argument('output_path', type=str, help='Output path for the processed CSV file')
    args = parser.parse_args()
    main(args.csvdata, args.csvmapping, args.output_path)
