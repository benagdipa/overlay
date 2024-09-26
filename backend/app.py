
import os
import logging
import pandas as pd
from flask import Flask, request, jsonify, send_file
from io import StringIO, BytesIO
from werkzeug.utils import secure_filename

# Initialize Flask app
app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = './uploads/'
ALLOWED_EXTENSIONS = {'csv', 'xlsx'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Ensure the upload folder exists
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Utility function to check allowed file extensions
def allowed_file(filename):
    """
    Check if the file extension is allowed.

    Args:
        filename (str): The name of the file to check.

    Returns:
        bool: True if the file extension is allowed, False otherwise.
    """
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """
    Handle CSV and Excel file uploads. Saves the file to the server.

    Returns:
        JSON response indicating success or failure of the upload.
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part in the request'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            logging.info(f"File uploaded: {filename}")
            return jsonify({'message': f'File uploaded successfully: {filename}'}), 200
        else:
            return jsonify({'error': 'File type not allowed'}), 400

    except Exception as e:
        logging.error(f"Error during file upload: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/data/<filename>', methods=['GET'])
def get_data(filename):
    """
    Load and display data from a CSV or Excel file.

    Args:
        filename (str): The name of the file to load.

    Returns:
        JSON response containing the data from the file.
    """
    try:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if not os.path.exists(filepath):
            return jsonify({'error': f'File {filename} not found'}), 404
        
        # Determine file type and read accordingly
        if filename.endswith('.csv'):
            data = pd.read_csv(filepath)
        elif filename.endswith('.xlsx'):
            data = pd.read_excel(filepath)
        else:
            return jsonify({'error': 'Unsupported file format'}), 400

        return jsonify(data.to_dict(orient='records')), 200

    except Exception as e:
        logging.error(f"Error reading file {filename}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/<filename>', methods=['GET'])
def export_data(filename):
    """
    Export the data as either a CSV or Excel file.

    Args:
        filename (str): The name of the file to export.

    Returns:
        The file as a download.
    """
    try:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if not os.path.exists(filepath):
            return jsonify({'error': f'File {filename} not found'}), 404
        
        # Determine file type and read accordingly
        if filename.endswith('.csv'):
            data = pd.read_csv(filepath)
            output = StringIO()
            data.to_csv(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='text/csv', as_attachment=True, attachment_filename=filename)

        elif filename.endswith('.xlsx'):
            data = pd.read_excel(filepath)
            output = BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            data.to_excel(writer, index=False)
            writer.save()
            output.seek(0)
            return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             as_attachment=True, attachment_filename=filename)
        else:
            return jsonify({'error': 'Unsupported file format'}), 400

    except Exception as e:
        logging.error(f"Error exporting file {filename}: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False)
