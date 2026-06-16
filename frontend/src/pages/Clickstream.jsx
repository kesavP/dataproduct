import { useState } from 'react'
import { databricksAPI } from '../services/api'
import './Clickstream.css'

function Clickstream() {
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [query, setQuery] = useState('SELECT * FROM dev.retail.clickstream LIMIT 10')

  const handleExecuteQuery = async () => {
    try {
      setLoading(true)
      setError(null)
      const response = await databricksAPI.executeQuery(query)
      setData(response.data.result?.data_array || [])
    } catch (err) {
      setError(err.message || 'Failed to execute query')
      setData([])
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="clickstream">
      <h2>Databricks Query Executor</h2>

      <div className="query-editor">
        <label>SQL Query:</label>
        <textarea
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Enter your SQL query here..."
          rows={6}
        />
        <button
          onClick={handleExecuteQuery}
          disabled={loading}
          className="execute-btn"
        >
          {loading ? 'Executing...' : 'Execute Query'}
        </button>
      </div>

      {error && (
        <div className="error">
          <strong>Error:</strong> {error}
        </div>
      )}

      {data.length > 0 && (
        <div className="results">
          <h3>Results ({data.length} rows)</h3>
          <div className="table-wrapper">
            <table className="results-table">
              <thead>
                <tr>
                  {data[0] && Object.keys(data[0]).map(key => (
                    <th key={key}>{key}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.map((row, idx) => (
                  <tr key={idx}>
                    {Object.values(row).map((val, cidx) => (
                      <td key={cidx}>{String(val)}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {!loading && !error && data.length === 0 && (
        <div className="placeholder">
          Enter a query and click Execute to see results
        </div>
      )}
    </div>
  )
}

export default Clickstream
