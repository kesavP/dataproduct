import { useEffect, useState } from 'react'
import { pipelineAPI, ordersAPI } from '../services/api'
import './Dashboard.css'

function Dashboard() {
  const [metrics, setMetrics] = useState(null)
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true)
        const [metricsRes, summaryRes] = await Promise.all([
          pipelineAPI.getMetrics(),
          ordersAPI.getSummary()
        ])
        setMetrics(metricsRes.data)
        setSummary(summaryRes.data)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchData()
  }, [])

  if (loading) return <div className="loading">Loading...</div>
  if (error) return <div className="error">Error: {error}</div>

  return (
    <div className="dashboard">
      <h2>Pipeline Metrics</h2>
      
      <div className="metrics-grid">
        <div className="metric-card">
          <h3>Bronze (Ingested)</h3>
          <p className="metric-value">{metrics?.bronze_records_ingested || 0}</p>
        </div>
        <div className="metric-card">
          <h3>Silver (Cleaned)</h3>
          <p className="metric-value">{metrics?.silver_records_cleaned || 0}</p>
        </div>
        <div className="metric-card">
          <h3>DLQ (Failed)</h3>
          <p className="metric-value">{metrics?.dlq_records_failed || 0}</p>
        </div>
        <div className="metric-card">
          <h3>Status</h3>
          <p className="metric-value">{metrics?.pipeline_status || 'Unknown'}</p>
        </div>
      </div>

      <div className="summary-section">
        <h2>Order Summary</h2>
        <div className="summary-grid">
          <div className="summary-item">
            <label>Total Orders:</label>
            <span>{summary?.total_orders || 0}</span>
          </div>
          <div className="summary-item">
            <label>Total Amount:</label>
            <span>€{(summary?.total_amount || 0).toFixed(2)}</span>
          </div>
          <div className="summary-item">
            <label>Average Amount:</label>
            <span>€{(summary?.avg_amount || 0).toFixed(2)}</span>
          </div>
        </div>
      </div>
    </div>
  )
}

export default Dashboard
