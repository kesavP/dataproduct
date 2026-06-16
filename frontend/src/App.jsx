import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import Bronze from './pages/Bronze'
import Orders from './pages/Orders'
import DLQ from './pages/DLQ'
import Clickstream from './pages/Clickstream'
import './App.css'

function App() {
  return (
    <Router>
      <Layout>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/bronze" element={<Bronze />} />
          <Route path="/orders" element={<Orders />} />
          <Route path="/dlq" element={<DLQ />} />
          <Route path="/clickstream" element={<Clickstream />} />
        </Routes>
      </Layout>
    </Router>
  )
}

export default App
