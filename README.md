
# Full-Stack Next.js & Flask Project

## Overview

This project demonstrates a full-stack web application built with **Next.js** for the frontend and **Flask** for the backend. The app allows users to upload CSV/Excel files, view the contents in a table format, and export the processed data back in CSV/Excel format.

### Tech Stack

- **Backend**: Flask (Python)
- **Frontend**: Next.js (React)
- **APIs**: RESTful APIs for file upload, data retrieval, and export
- **File Support**: CSV and Excel

## Features

- Upload CSV or Excel files.
- Display uploaded data in a table.
- Download the processed data as CSV or Excel.
- Fully responsive design with clear UI and error handling.

## Project Structure

```
/backend
  ├── app.py             # Flask app with APIs for upload, data display, and export
/frontend
  ├── pages
    ├── upload.js        # Next.js page for file upload, display, and export
  ├── package.json       # Frontend package dependencies
```

## Setup

### Backend (Flask)

1. Navigate to the backend directory:
   ```bash
   cd backend
   ```

2. Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the Flask server:
   ```bash
   python app.py
   ```

   The backend will be running on `http://localhost:5000`.

### Frontend (Next.js)

1. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```

2. Install the required Node.js dependencies:
   ```bash
   npm install
   ```

3. Run the Next.js development server:
   ```bash
   npm run dev
   ```

   The frontend will be running on `http://localhost:3000`.

## Usage

1. Navigate to `http://localhost:3000/upload` in your browser.
2. Upload a CSV or Excel file.
3. View the data in a table format.
4. Export the data in CSV or Excel format.

## API Endpoints

- `POST /api/upload` : Upload a CSV or Excel file.
- `GET /api/data/<filename>` : Retrieve the data from the uploaded file.
- `GET /api/export/<filename>` : Export the data as CSV or Excel.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
