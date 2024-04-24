import csv
import argparse

# Definir la ruta al archivo CSV y al archivo de salida YARRRML
#csv_file_name = '1_mappings_data.csv'
#csv_file_path = '/Users/catimc/Documents/PROYECTOS/RES-Q+/TRANSFORMATION/data/'+csv_file_name 
#output_file_path = '/Users/catimc/Documents/PROYECTOS/RES-Q+/TRANSFORMATION/data/reglasgenericas.yarrrml'



def generate_observation_result_statement(row,csv_file_name):
    rule_name = row['ontology_mapping'].strip() + '_' + row['field_id'].strip()
    ontology_mapping = row['ontology_mapping'].strip()
    field_id = row['field_id'].strip()
    observable = row['observable'].strip()
    source_procedure = row['source_procedure'].split('/')[-1] if row['source_procedure'] else 'unknown_procedure'
    statement_context = row.get('statement_context', '').strip()
    temporal_context = row.get('temporal_context', '').strip()
    value_type = row['value_type'].strip()
    field_value = row['field_value'].strip()
    
    regla = f"""
        {rule_name}_ObservationResultStatement:
            sources:
                - ['{csv_file_name}~csv']
            s: base:{ontology_mapping}_{field_id}_$(case_id)
            po:
                - [a, resqplus:{ontology_mapping}~iri]
                - [scdm:hasObservable, {observable}~iri] # observable entity
                - [scdm:isResultOf, base:Procedure_{source_procedure}_$(case_id)~iri]
        """
    
    # Handling value types other than Boolean
    if value_type != 'Boolean':
        regla += f"        - [scdm:hasObservableValue, base:PrimitiveValue_{field_id}_$(case_id)~iri]\n"

    # Handling Boolean values and adding context
    if value_type == 'Boolean':
        if field_value == 'FALSE' and statement_context:
            regla += f"        - [scdm:situationContext, {statement_context}~iri]\n"
        elif statement_context:
            regla += f"        - [scdm:situationContext, {statement_context}~iri]\n"
        regla += generate_statement_context(row,statement_context,rule_name,csv_file_name)

    # Adding temporal context if available
    if temporal_context:
        regla += f"            - [scdm:temporalContext, {temporal_context}~iri]\n"
        regla+=generate_statement_context(row,temporal_context,rule_name,csv_file_name)

    return regla

def generate_procedure(row,csv_file_name):
    rule_name = row['ontology_mapping'].strip()
    source_procedure = row['source_procedure'].strip()

    # If the source procedure is empty, return empty to avoid generating invalid rules
    if not source_procedure:
        return ''

    # Extract the last part of the source procedure which is typically used as an identifier
    if '#' in source_procedure:
        source_procedure, source_procedure_iri = source_procedure.split('#')[-1], source_procedure
    else:
        source_procedure, source_procedure_iri = source_procedure.split('/')[-1], source_procedure

    # Base rule setup
    regla = f"""
        {rule_name}_ResultOfProcedure:
            sources: 
                - ['{csv_file_name}~csv']
            s: base:Procedure_{source_procedure}_$(case_id)
            po:
                - [a, {source_procedure_iri}~iri]
    """

    # Handling procedure parts if they exist
    procedure_part = row['procedure_part'].strip()
    if procedure_part:
        if '#' in procedure_part:
            procedure_part = procedure_part.split('#')[-1]
        else:
            procedure_part = procedure_part.split('/')[-1]

        # Link the procedure part as a sub-process of the main procedure
        regla += f"            - [scdm:isSubProcessOf, base:Procedure_{procedure_part}_$(case_id)~iri]\n"

    return regla


def generate_observation_result(row,csv_file_name):
    rule_name = row['ontology_mapping'].strip() + '_' + row['field_id'].strip()
    field_id = row['field_id'].strip()
    field_value = row['field_value'].strip()
    value_type = row['value_type'].strip().lower()

    # Mapping value types to XSD types
    xsd_types = {
        'integer': 'xsd:integer',
        'float': 'xsd:double',
        'datetime': 'xsd:dateTime',
        'string': 'xsd:string',  
        'boolean': 'xsd:boolean'
    }

    # Default to xsd:string if datatype is not recognized
    datatype_value = xsd_types.get(value_type, 'xsd:string')

    regla = f"""
        {rule_name}_PrimitiveValue:
            sources: 
                - ['{csv_file_name}~csv']
            s: base:PrimitiveValue_{field_id}_$(case_id)
            po:
                - [a, scdm:QuantitativeResultValue~iri]
                - [btl2:hasValue, $(field_value), {datatype_value}]  
        """
    return regla

def generate_clinical_situation_statement(row,csv_file_name):
    ontology_mapping = row['ontology_mapping'].strip()
    field_id = row['field_id'].strip()
    source_procedure = row['source_procedure'].strip().split('/')[-1]
    finding = row['finding'].strip()
    rule_name = ontology_mapping+'_'+field_id
    field_value = row['field_value'].strip().upper()
    value_type = row['value_type'].strip()
    temporal_context = row['temporal_context'].strip()
    additional_rules = ''

    # Definir las claves que comparten el mismo valor en listas
    keys_for_true = ['TRUE', 'VERDADERO']
    keys_for_false = ['FALSE', 'FALSO']

    # Crear el diccionario usando comprensión de diccionario
    situation_context_map = {key: "sct:410515003~iri" for key in keys_for_true}
    situation_context_map.update({key: "sct:410516002~iri" for key in keys_for_false})


    #regla = f"""
     #   {rule_name}_ClinicalSituationStatement:
      #      sources: 
       #         - ['{csv_file_name}~csv']
        #    s: base:ClinicalSituationSt_{ontology_mapping}_{field_id}_$(case_id)
         #   po:
          #      - [a, resqplus:{ontology_mapping}~iri]
           #     - [scdm:isResultOf, base:Procedure_{source_procedure}_$(case_id)~iri]
            #    - p: scdm:temporalContext
             #     o:
              #      functionValue:
               #         function: http://localhost:5001/addTemporalContext
                #        parameters:
                 #           ex:temporalContext: $(temporal_context)
        #"""
    regla = f"""
        {rule_name}_ClinicalSituationStatement:
            sources: 
                - ['{csv_file_name}~csv']
            s: base:ClinicalSituationSt_{ontology_mapping}_{field_id}_$(case_id)
            po:
                - [a, resqplus:{ontology_mapping}~iri]
                - [scdm:isResultOf, base:Procedure_{source_procedure}_$(case_id)~iri]
        """

    # Añadir contexto de situación basado en field_value
    if field_value in situation_context_map:
        regla += f"        - [scdm:situationContext, {situation_context_map[field_value]}]\n"
        additional_rules +=  generate_statement_context(row,situation_context_map[field_value],rule_name,csv_file_name)


    if value_type == 'Categorical':
        regla += f"        - [scdm:representsSituation, $(categorical_ontology_mapping)~iri]\n"
    elif value_type == 'Boolean':
        regla += f"                - [scdm:representsSituation, $(finding)~iri]\n"

    procedure_result = row['procedure_result'].strip()
    if procedure_result:
        procedure = procedure_result
        procedure_result = procedure_result.split('#')[-1] if '#' in procedure_result else procedure_result.split('/')[-1]
        regla += f"                - [scdm:isResultOf, base:Procedure_$(procedure_result)_$(case_id)~iri]\n"
        additional_rules+=generate_resultProcedure(row,procedure,rule_name,csv_file_name)

    temporal_context = row['temporal_context'].strip()
    if temporal_context:
        regla += f"                - [scdm:temporalContext, {temporal_context}~iri]\n"
        additional_rules+=generate_statement_context(row,temporal_context,rule_name,csv_file_name)
    return regla+additional_rules

def generate_resultProcedure(row,procedure,rule_name,csv_file_name):
    regla = f"""
        {rule_name}_ResultOfProcedure:
            sources: 
                - ['{csv_file_name}~csv']
            s: base:Procedure_$(procedure_result)_$(case_id)
            po:
                - [a, $(procedure_result)~iri]
    """
    return regla


def generate_statement_context(row, statement_context,rule_name,csv_file_name):
    regla = f"""
        {rule_name}_StatementContext:
            sources: 
                - ['{csv_file_name}~csv']
            s: {statement_context}
            po:
                - [a, {statement_context}]
    """
    return regla

def generate_clinical_procedure_statement(row,csv_file_name):
    ontology_mapping = row['ontology_mapping'].strip()
    field_id = row['field_id'].strip()
    source_procedure = extract_last_part(row['source_procedure'].strip())
    rule_name = f"{ontology_mapping}_{field_id}_ClinicalProcedureStatement"
    additional_rules = ''
    regla = f"""
        {rule_name}:
            sources:
                - ['{csv_file_name}~csv']
            s: base:ClinicalProcedureSt_{ontology_mapping}_{field_id}_$(case_id)
            po:
                - [a, resqplus:{ontology_mapping}~iri]
                - [scdm:isResultOf, base:Procedure_{source_procedure}_$(case_id)~iri]
        """

    # Incorporate procedure context
    context_uri = determine_procedure_context(row,csv_file_name)
    if context_uri:
        regla += f"        - [scdm:procedureContext, {context_uri}]\n"
        additional_rules+=generate_statement_context(row,context_uri,rule_name,csv_file_name)

    # Include additional attributes based on procedure
    if row.get('procedure'):
        regla += generate_procedure_extension(row,csv_file_name)

    return regla+additional_rules

def determine_procedure_context(row,csv_file_name):
    field_value = row['field_value'].strip()
    statement_context = row.get('statement_context', '').strip()
    categorical_ontology_mapping = row.get('categorical_ontology_mapping', '').strip()

    # Mapping para los valores que no dependen del statement_context
    context_map = {
        'not required': f"{categorical_ontology_mapping}~iri",
        'recommended only': f"{categorical_ontology_mapping}~iri",
        'yes': "sct:85658003~iri",  # done
        'no': "sct:385660001~iri",  # not done
        'not applicable': "sct:385432009~iri"  # not applicable
    }

    # Manejo especial para valores booleanos que pueden depender de 'statement_context'
    if field_value in ('TRUE', 'VERDADERO'):
        if statement_context:
            return "sct:385660001~iri"  # not done
        return "sct:85658003~iri"  # done
    elif field_value in ('FALSE', 'FALSO'):
        if statement_context:
            return "sct:85658003~iri"  # done
        return "sct:385660001~iri"  # not done

    # Retorna el contexto adecuado para otros casos normales
    return context_map.get(field_value)

def generate_procedure_extension(row,csv_file_name):
    value_type = row['value_type'].strip()
    regla = ""

    if value_type == 'Categorical' or value_type == '': 
        # Procesar basado en el tipo de procedimiento especificado
        procedure_type = row['procedure'].strip()
        categorical_ontology_mapping = row['categorical_ontology_mapping'].strip()
        field_value = row['field_value'].strip().lower()
        procedure_key = {
            'procedureReason': 'procedure_reason',
            'performer': 'categorical_ontology_mapping',
            'procedureLocation': 'procedure_location'
        }.get(procedure_type, 'categorical_ontology_mapping')

        # Extracción del detalle relevante para el tipo de procedimiento
        procedure_detail = extract_last_part(row.get(procedure_key, '').strip())
        extension_type = {
            'procedureReason': "procedureReason",
            'performer': "hasInformationAboutProvider",
            'procedureLocation': "procedureLocation"
        }.get(procedure_type, "representsProcedure")

        if extension_type == "representsProcedure" and not categorical_ontology_mapping:
            regla += f"        - [scdm:{extension_type}, base:Procedure_$(procedure)_$(case_id)~iri]\n"
        elif extension_type == "representsProcedure" and field_value in ['yes','no']:
            regla += f"                - [scdm:{extension_type}, base:Procedure_$(procedure)_$(case_id)~iri]\n"
        else:
            # Generar regla usando el detalle extraído
            regla += f"        - [scdm:{extension_type}, base:{extension_type.capitalize()}_$({procedure_key})_$(case_id)~iri]\n"

    elif value_type == 'Boolean':
        # Manejar valores booleanos extrayendo y formando regla genérica
        procedure_detail = extract_last_part(row.get('procedure', '').strip())
        regla += f"                - [scdm:representsProcedure, base:Procedure_{procedure_detail}_$(case_id)~iri]\n"

    return regla

def generate_represented_procedure(row,csv_file_name):
    ontology_mapping = row['ontology_mapping'].strip()
    value_type = row['value_type'].strip()
    
    # Determinar el origen del URI y procesar según el tipo de valor
    uri_source = 'categorical_ontology_mapping' if value_type == 'Categorical' else 'procedure'
    procedure_uri = row[uri_source].strip()
    
    # Utilizar la función existente para extraer el nombre del procedimiento
    sol_procedure = extract_last_part(procedure_uri)
    
    # Formar el URI del procedimiento basado en el tipo de valor
    sol_procedure_uri = f"resqplus:{sol_procedure}" if value_type == 'Categorical' and '#' in procedure_uri else procedure_uri
    
    # Formar el nombre de la regla combinando el mapeo de la ontología con el nombre del procedimiento
    rule_name = f"{ontology_mapping}_{sol_procedure}"
    field_value = row['field_value'].strip().lower()
    if value_type == 'Categorical' and field_value not in ['yes','no']:
        subject = f"base:Procedure_$(categorical_ontology_mapping)_$(case_id)"
        object = f"categorical_ontology_mapping"
    else:    
        subject= f"base:Procedure_$(procedure)_$(case_id)"
        object = f"procedure"

    # Construir la regla YARRRML si el URI del procedimiento ha sido definido
    if sol_procedure_uri:
        regla = f"""
        {rule_name}_RepresentedProcedure:
            sources: 
                - ['{csv_file_name}~csv']
            s: {subject}~iri
            po:
                - [a, $({object})~iri]
        """
        return regla
    return ""

def extract_last_part(uri):
    """Extracts the last significant part of a URI, handling both '#' and '/' separators."""
    return uri.split('#')[-1] if '#' in uri else uri.split('/')[-1]

def generate_specified_date_time(row,csv_file_name):
    ontology_mapping = row['ontology_mapping'].strip()
    rule_name = ontology_mapping+'_SpecifiedTime'
    field_id = row['field_id'].strip()
    regla=f"""
        {rule_name}:
            sources: 
                - ['{csv_file_name}~csv']
            s: base:SpecifiedTime_{ontology_mapping}_$(case_id)
            po:
                - [a, sct:410512000~iri]
                - [scdm:hasPart, base:PrimitiveValue_{field_id}_$(case_id)~iri]
        """
    return regla

def generate_procedure_attribute(row, attribute, ontology_suffix,csv_file_name):
    """Generates YARRRML rules for specific procedure attributes like location, reason, or performer."""
    attribute_value = row[attribute].strip()
    if not attribute_value:
        return ''
    last_part = extract_last_part(attribute_value)
    rule_name = f"{row['ontology_mapping'].strip()}_{ontology_suffix}"

    if attribute == 'categorical_ontology_mapping':
        attribute_uri = f"scdm:InformationAboutProviderOfInformation"  # Usa la variable directamente en la regla
    else:
        attribute_uri = f"$({attribute})~iri"  # Usa la variable con el sufijo IRI para otros atributos

    regla= f"""
        {rule_name}_{ontology_suffix.capitalize()}:
            sources:
               - ['{csv_file_name}~csv']
            s: base:{ontology_suffix.capitalize()}_$({attribute})_$(case_id)
            po:
                - [a, {attribute_uri}]
            """
    if attribute == 'categorical_ontology_mapping':
        regla += f"    - [btl2:represents, base:Procedure_$({attribute})_$(case_id)~iri]\n"

    return regla
def generate_procedure_location(row,csv_file_name):
    return generate_procedure_attribute(row, 'procedure_location', 'procedureLocation',csv_file_name)

def generate_procedure_reason(row,csv_file_name):
    return generate_procedure_attribute(row, 'procedure_reason', 'procedureReason',csv_file_name)

def generate_procedure_performer(row,csv_file_name):
    if row['procedure'].strip() != 'performer':
        return ''
    return generate_procedure_attribute(row, 'categorical_ontology_mapping', 'hasInformationAboutProvider',csv_file_name)


def generate_rule(row, pattern_handlers,csv_file_name):
    """
    Llama a la función correspondiente al tipo de patrón.
    """
    pattern_type = row['pattern_type'].strip()
    rules = []
    if pattern_type in pattern_handlers:
        for handler in pattern_handlers[pattern_type]:
            rule = handler(row,csv_file_name)
            if rule:
                rules.append(rule)
        return "\n".join(rules)
    else:
        print(f"Tipo de patrón desconocido o sin manejador: {pattern_type}")  # Diagnóstico
        return None  # O manejar de alguna forma que no hay una función para ese tipo


def main():
    parser = argparse.ArgumentParser(description="Genera archivos YARRRML a partir de un CSV")
    parser.add_argument('--input', type=str, help='Ruta completa al archivo CSV de entrada')
    parser.add_argument('--output', type=str, help='Ruta completa al archivo YARRRML de salida')
    args = parser.parse_args()

    csv_file_path = args.input
    output_file_path = args.output
    csv_file_name = csv_file_path.split('/')[-1]

    # Diccionario de funciones para manejar distintos tipos de patrones
    pattern_handlers = {
        'ObservationResultStatement': [generate_observation_result_statement, generate_procedure, generate_observation_result],
        'ClinicalSituationStatement': [generate_clinical_situation_statement,generate_procedure],
        'ClinicalProcedureStatement': [generate_clinical_procedure_statement,generate_represented_procedure,generate_procedure_reason, generate_procedure_performer, generate_procedure_location, generate_procedure ]
    }

    yarrml_template = """
    authors: Catalina Martinez-Costa <cmartinezcosta@um.es>
    prefixes:
      base: http://resqplus-resources/ontologies/resqplus-data#
      resqplus: http://www.semanticweb.org/catimc/resqplus#
      sct: http://snomed.info/id/
      scdm: http://www.semanticweb.org/catimc/SemanticCommonDataModel#
      btl2: http://purl.org/biotop/btl2.owl#
      fno:  https://w3id.org/function/ontology# 
      fnom: https://w3id.org/function/vocabulary/mapping#
      ex: http://example.org/functions#
    
    functions:
        ex:addTemporalContext:
            function: "ex:addTemporalContext"
            mappings:
            input:
                - [fno:predicate, fno:input]
            output:
                - [fno:predicate, fno:output]
            implementation:
            types:
                - "fnom:WebService"
            endpoint: "http://localhost:5001/addTemporalContext"
            method: "POST"

    mappings:
    """
    rules = []
    field_ids_seen = set()

    with open(csv_file_path, mode='r', encoding='utf-8-sig') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            field_id = row['field_id'].strip()
            if field_id in field_ids_seen:
                break  # Detener el procesamiento si el field_id ya ha sido visto
            field_ids_seen.add(field_id)
            rule = generate_rule(row, pattern_handlers,csv_file_name)
            if rule:
                rules.append(rule)

    # Combinar la plantilla con los mapeos generados
    yarrml_output = yarrml_template + '\n'.join(rules)

    # Escribir el YARRRML generado en el archivo
    with open(output_file_path, 'w') as output_file:
        output_file.write(yarrml_output)

    print(f"YARRRML generado guardado en: {output_file_path}")

if __name__ == "__main__":
    main()