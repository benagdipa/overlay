
import { useState } from 'react';
import axios from 'axios';

export default function Upload() {
  const [file, setFile] = useState(null);
  const [message, setMessage] = useState('');
  const [data, setData] = useState(null);

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
  };

  const handleUpload = async () => {
    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await axios.post('/api/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });
      setMessage(response.data.message);
      fetchData(file.name);
    } catch (error) {
      setMessage('Error uploading file');
    }
  };

  const fetchData = async (filename) => {
    try {
      const response = await axios.get(`/api/data/${filename}`);
      setData(response.data);
    } catch (error) {
      setMessage('Error fetching data');
    }
  };

  const handleExport = async (filename) => {
    try {
      const response = await axios.get(`/api/export/${filename}`, {
        responseType: 'blob',
      });
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', filename); 
      document.body.appendChild(link);
      link.click();
    } catch (error) {
      setMessage('Error exporting data');
    }
  };

  return (
    <div className="container">
      <h2 className="text-center">Upload and View Data (CSV/Excel)</h2>
      <input type="file" onChange={handleFileChange} />
      <button onClick={handleUpload} className="btn btn-primary mt-2">Upload</button>

      {message && <p>{message}</p>}

      {data && (
        <div>
          <h3>Uploaded Data</h3>
          <table className="table table-striped">
            <thead>
              <tr>
                {Object.keys(data[0]).map((key) => (
                  <th key={key}>{key}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((row, index) => (
                <tr key={index}>
                  {Object.values(row).map((value, idx) => (
                    <td key={idx}>{value}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>

          <button onClick={() => handleExport(file.name)} className="btn btn-success mt-3">Export Data</button>
        </div>
      )}
    </div>
  );
}
