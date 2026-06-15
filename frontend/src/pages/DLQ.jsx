import { useEffect, useState } from 'react'
import { pipelineAPI } from '../services/api'
import './DLQ.css'

function DLQ() {
  const [records, setRecords] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    const fetchDLQ = async () => {
      try {
        setLoading(true)
        const res = await pipelineAPI.getDLQ(100)
        setRecords(res.data)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchDLQ()
  }, [])

  if (loading) return <div className="loading">Loading...</div>
  if (error) return <div className="error">Error: {error}</div>

  return (
    <div className="dlq">
      <h2>Dead Letter Queue (DLQ)</h2>
      <p className="description">
        Records that failed transformation in the silver layer are stored here for investigation.
      </p>
      
      {records.length === 0 ? (
        <div className="empty-state">No records in DLQ - Pipeline is healthy!</div>
      ) : (
        <div className="table-wrapper">
          <table className="dlq-table">
            <thead>
              <tr>
                <th>Order ID</th>
                <th>Rule ID</th>
                <th>Error Type</th>
                <th>Error Message</th>
                <th>Captured At</th>
              </tr>
            </thead>
            <tbody>
              {records.map((record, idx) => (
                <tr key={idx}>
                  <td>{record.order_id}</td>
                  <td>{record.transformation_rule_id}</td>
                  <td>{record.error_type}</td>
                  <td>{record.error_message}</td>
                  <td>{record.captured_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

export default DLQ
