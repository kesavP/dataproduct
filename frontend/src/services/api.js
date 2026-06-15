import axios from 'axios'

const API_BASE_URL = '/api'

const apiClient = axios.create({
  baseURL: API_BASE_URL,
})

export const ordersAPI = {
  getSummary: () => apiClient.get('/orders/summary'),
  getOrders: (limit = 100, offset = 0) => 
    apiClient.get('/orders', { params: { limit, offset } }),
}

export const pipelineAPI = {
  getMetrics: () => apiClient.get('/pipeline/metrics'),
  getDLQ: (limit = 100) => 
    apiClient.get('/pipeline/dlq', { params: { limit } }),
}

export default apiClient
