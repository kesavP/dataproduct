import { Link } from 'react-router-dom'
import './Layout.css'

function Layout({ children }) {
  return (
    <div className="layout">
      <nav className="navbar">
        <div className="nav-container">
          <h1 className="nav-logo">Retail Data Dashboard</h1>
          <ul className="nav-menu">
            <li className="nav-item">
              <Link to="/" className="nav-link">Dashboard</Link>
            </li>
            <li className="nav-item">
              <Link to="/bronze" className="nav-link">Bronze</Link>
            </li>
            <li className="nav-item">
              <Link to="/orders" className="nav-link">Orders</Link>
            </li>
            <li className="nav-item">
              <Link to="/dlq" className="nav-link">DLQ</Link>
            </li>
            <li className="nav-item">
              <Link to="/clickstream" className="nav-link">Clickstream</Link>
            </li>
          </ul>
        </div>
      </nav>
      <main className="main-content">
        {children}
      </main>
    </div>
  )
}

export default Layout
