import { useEffect, useState } from 'react'
import { ordersAPI } from '../services/api'
import './Orders.css'

function Orders() {
  const [orders, setOrders] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [page, setPage] = useState(0)

  useEffect(() => {
    const fetchOrders = async () => {
      try {
        setLoading(true)
        const res = await ordersAPI.getOrders(100, page * 100)
        setOrders(res.data)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchOrders()
  }, [page])

  if (loading) return <div className="loading">Loading...</div>
  if (error) return <div className="error">Error: {error}</div>

  return (
    <div className="orders">
      <h2>Cleaned Orders</h2>
      <div className="table-wrapper">
        <table className="orders-table">
          <thead>
            <tr>
              <th>Order ID</th>
              <th>Date</th>
              <th>Customer</th>
              <th>Amount (EUR)</th>
              <th>Campaign</th>
              <th>Payment Status</th>
              <th>Product</th>
            </tr>
          </thead>
          <tbody>
            {orders.map(order => (
              <tr key={order.order_id}>
                <td>{order.order_id}</td>
                <td>{order.order_date}</td>
                <td>{order.customer_name}</td>
                <td>€{order.total_amount_eur.toFixed(2)}</td>
                <td>{order.campaign_id}</td>
                <td>{order.payment_status}</td>
                <td>{order.product_id}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="pagination">
        <button disabled={page === 0} onClick={() => setPage(page - 1)}>
          Previous
        </button>
        <span>Page {page + 1}</span>
        <button onClick={() => setPage(page + 1)} disabled={orders.length === 0}>
          Next
        </button>
      </div>
    </div>
  )
}

export default Orders
