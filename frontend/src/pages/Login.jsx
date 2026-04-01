import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'
import ThemeToggle from '../components/ThemeToggle'
import { HeartIcon, MapPinIcon, ShieldIcon } from '../components/Icons'

const API = import.meta.env.VITE_API_URL
const LAST_USERNAME_STORAGE_KEY = 'casinder-last-username'

const TRUST_HIGHLIGHTS = [
  {
    title: 'Ricerca piu mirata',
    text: 'Filtri rapidi, raggio e preferenze utili senza schermate pesanti.',
    icon: MapPinIcon,
  },
  {
    title: 'Salvataggi immediati',
    text: 'Tieni da parte gli annunci migliori e riguardali con calma.',
    icon: HeartIcon,
  },
  {
    title: 'Accesso leggero',
    text: 'Entri con uno username locale, senza password o frizione inutile.',
    icon: ShieldIcon,
  },
]

function getInitialUsername() {
  if (typeof window === 'undefined') {
    return ''
  }

  return window.localStorage.getItem(LAST_USERNAME_STORAGE_KEY) || ''
}

export default function Login() {
  const [username, setUsername] = useState(getInitialUsername)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const navigate = useNavigate()

  async function handleSubmit(event) {
    event.preventDefault()

    const normalizedUsername = username.trim()
    if (!normalizedUsername) {
      return
    }

    setLoading(true)
    setError('')

    try {
      const res = await axios.post(`${API}/utenti/login`, { username: normalizedUsername })
      localStorage.setItem('utente', JSON.stringify(res.data))
      localStorage.setItem(LAST_USERNAME_STORAGE_KEY, normalizedUsername)
      navigate('/swipe')
    } catch {
      setError('Errore di connessione. Riprova.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="auth-view">
      <section className="auth-card">
        <div className="auth-toolbar">
          <ThemeToggle compact />
        </div>

        <div className="auth-layout">
          <div className="auth-copy">
            <span className="auth-kicker">Affitti a colpo d'occhio</span>
            <h1 className="auth-title">Casinder</h1>
            <p className="auth-subtitle">
              Scopri case e stanze con il ritmo di una dating app, ma con piu contesto,
              meno rumore e filtri che aiutano davvero.
            </p>

            <div className="auth-pills">
              <span>Swipe mirato</span>
              <span>Filtri smart</span>
              <span>Annunci salvati</span>
            </div>

            <div className="auth-highlight-grid">
              {TRUST_HIGHLIGHTS.map((item) => {
                const Icon = item.icon

                return (
                  <article key={item.title} className="auth-highlight">
                    <span className="auth-highlight__icon">
                      <Icon />
                    </span>
                    <div>
                      <strong>{item.title}</strong>
                      <p>{item.text}</p>
                    </div>
                  </article>
                )
              })}
            </div>

            <p className="auth-note">
              Nessuna password: usiamo solo uno username locale per iniziare, e puoi cambiare
              profilo quando vuoi.
            </p>
          </div>

          <div className="auth-panel">
            <form onSubmit={handleSubmit} className="auth-form">
              <label className="filter-input" htmlFor="username">
                <span className="filter-section__label">Username</span>
                <input
                  id="username"
                  className="auth-input"
                  type="text"
                  placeholder="Scegli un username"
                  value={username}
                  onChange={(event) => setUsername(event.target.value)}
                  maxLength={30}
                  autoComplete="nickname"
                  autoFocus
                  required
                  aria-describedby="auth-helper"
                />
              </label>

              {error && <p className="auth-error">{error}</p>}

              <button className="auth-button" type="submit" disabled={loading || !username.trim()}>
                {loading ? 'Accesso in corso...' : 'Entra in Casinder'}
              </button>

              <p id="auth-helper" className="auth-helper">
                Ti basta un nome per iniziare. Il resto dell'esperienza si costruisce mentre navighi.
              </p>
            </form>
          </div>
        </div>
      </section>
    </div>
  )
}
