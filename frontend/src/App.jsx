import { Routes, Route, Navigate } from 'react-router-dom'
import './App.css'
import Login from './pages/Login'
import Swipe from './pages/Swipe'
import Preferiti from './pages/Preferiti'
import Profile from './pages/Profile'

function App() {
  return (
    <Routes>
      <Route path="/" element={<Login />} />
      <Route path="/swipe" element={<Swipe />} />
      <Route path="/cerca" element={<Navigate to="/swipe" replace />} />
      <Route path="/preferiti" element={<Preferiti />} />
      <Route path="/profile" element={<Profile />} />
      <Route path="*" element={<Navigate to="/" />} />
    </Routes>
  )
}

export default App
