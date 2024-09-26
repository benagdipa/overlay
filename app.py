
import logging
from flask import Flask, request, jsonify, render_template, send_file, redirect, url_for
import pandas as pd
import os
from io import StringIO
from werkzeug.utils import secure_filename

# Initialize the Flask app
app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = './uploads/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
ALLOWED_EXTENSIONS = {'csv'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB file upload limit

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Ensure the upload folder exists
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

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

@app.route('/')
def index():
    """
    Serve the main page, which allows the user to upload and export files.
    
    Returns:
        The main HTML page with options for file import and export.
    """
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Handle CSV file uploads from the browser.

    Returns:
        A redirect to the data display page with the uploaded file.
    """
    try:
        if 'file' not in request.files:
            logging.warning("No file part in the request")
            return jsonify({"error": "No file part in the request"}), 400

        file = request.files['file']

        if file.filename == '':
            logging.warning("No selected file")
            return jsonify({"error": "No selected file"}), 400

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            logging.info(f"File uploaded successfully: {filename}")
            return redirect(url_for('get_data', filename=filename))
        else:
            logging.warning("File type not allowed")
            return jsonify({"error": "File type not allowed"}), 400

    except Exception as e:
        logging.error(f"Error during file upload: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/data/<filename>', methods=['GET'])
def get_data(filename):
    """
    Load and display the content of the uploaded CSV file in an HTML table.
    
    Args:
        filename (str): The name of the file to load.

    Returns:
        The HTML template with the data displayed in a table.
    """
    try:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File '{filename}' not found")

        data = pd.read_csv(filepath)
        logging.info(f"Data loaded from file: {filename}")
        return render_template('data_table.html', data=data.to_dict(orient='records'), headers=data.columns.values, filename=filename)
    
    except FileNotFoundError as e:
        logging.error(e)
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        logging.error(f"Error processing file {filename}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/export/<filename>', methods=['GET'])
def export_data(filename):
    """
    Export the data from the uploaded CSV file as a downloadable file.

    Args:
        filename (str): The name of the file to export.

    Returns:
        The CSV file as a download.
    """
    try:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File '{filename}' not found")
        
        data = pd.read_csv(filepath)
        output = StringIO()
        data.to_csv(output, index=False)
        output.seek(0)
        
        logging.info(f"File exported: {filename}")
        return send_file(output, mimetype='text/csv', as_attachment=True, attachment_filename=f'{filename}')
    
    except FileNotFoundError as e:
        logging.error(e)
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        logging.error(f"Error exporting file {filename}: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False)
