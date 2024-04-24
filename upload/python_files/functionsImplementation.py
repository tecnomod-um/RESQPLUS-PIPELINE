from flask import Flask, request, jsonify
app = Flask(__name__)

@app.route('/addTemporalContext', methods=['POST'])
def handle_request():
    content = request.json
    temporal_context = content['temporal_context']
    result = addTemporalContext(temporal_context)
    return jsonify({"result": result})

def addTemporalContext(temporal_context):
    print("Holaaaaa")
    if temporal_context and temporal_context.strip() != '':
        return f"[scdm:temporalContext, {temporal_context}~iri]"
    return ""

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
