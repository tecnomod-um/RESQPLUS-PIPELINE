# RESQPLUS-PIPELINE
Data pipeline for representing data according to the RERS-Q+ ontology
![Diagrama](DiagramaPipelineTransformacion.png)


## Steps in the Pipeline

### Step 1: Preprocessing

```bash
python3 preprocessing.py ../input_data/data-extended.csv ../input_data/mappings.csv ../preprocessed_data
```
### Step 2 - Step 6:

You will need RML-MAPPER: https://github.com/RMLio/rmlmapper-java/

```bash
python3 script.py ../preprocessed_data/preprocessed_data.csv
```
