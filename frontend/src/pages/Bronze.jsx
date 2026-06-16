import { useEffect, useState } from 'react'
import { bronzeAPI } from '../services/api'
import './Bronze.css'

function Bronze() {
  const [data, setData] = useState([])
  const [totalCount, setTotalCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [page, setPage] = useState(0)
  const pageSize = 50

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true)
        const countRes = await bronzeAPI.getCount()
        setTotalCount(countRes.data.count)

        const dataRes = await bronzeAPI.getData(pageSize, page * pageSize)
        setData(dataRes.data.data)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchData()
  }, [page])

  if (loading) return <div className="loading">Loading...</div>
  if (error) return <div className="error">Error: {error}</div>

  const columns = data.length > 0 ? Object.keys(data[0]) : []
  const totalPages = Math.ceil(totalCount / pageSize)

  return (
    <div className="bronze">
      <h2>Bronze Layer (Clickstream)</h2>
      <div className="stats">
        <p>Total Records: <strong>{totalCount.toLocaleString()}</strong></p>
        <p>Page {page + 1} of {totalPages || 1}</p>
      </div>
      <div className="table-wrapper">
        <table className="bronze-table">
          <thead>
            <tr>
              {columns.map(col => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, idx) => (
              <tr key={idx}>
                {columns.map(col => (
                  <td key={`${idx}-${col}`}>
                    {typeof row[col] === 'object'
                      ? JSON.stringify(row[col])
                      : String(row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="pagination">
        <button disabled={page === 0} onClick={() => setPage(page - 1)}>
          Previous
        </button>
        <span>Page {page + 1} of {totalPages || 1}</span>
        <button onClick={() => setPage(page + 1)} disabled={page >= totalPages - 1}>
          Next
        </button>
      </div>
    </div>
  )
}

export default Bronze
