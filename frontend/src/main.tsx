import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

// No StrictMode: in dev it remounts and tears down the WebSocket while it is still connecting.
createRoot(document.getElementById('root')!).render(<App />)
