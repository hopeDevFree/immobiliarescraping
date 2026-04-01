import { useState } from 'react'
import { useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'
import ThemeToggle from '../components/ThemeToggle'
import { CompassIcon, HeartIcon, MapPinIcon, SearchIcon, ShieldIcon } from '../components/Icons'
import {
  API_BASE_URL,
  getApiDisplayUrl,
  getApiErrorMessage,
  getLegacyUserId,
  getStoredAuthToken,
  getStoredUser,
  hasAuthenticatedSession,
  storeUserSession,
} from '../utils/api'

const API = API_BASE_URL
const LAST_USERNAME_STORAGE_KEY = 'casinder-last-username'

const TRUST_HIGHLIGHTS = [
  {
    title: 'Feed piu leggibile',
    text: 'Scorri solo annunci ripuliti e pronti da confrontare.',
    icon: SearchIcon,
  },
  {
    title: 'Zone subito chiare',
    text: 'Parti da Napoli o stringi il raggio quando hai gia un punto preciso.',
    icon: MapPinIcon,
  },
  {
    title: 'Preferiti immediati',
    text: 'Salva tutto quello che vuoi rivedere, senza account pesanti.',
    icon: HeartIcon,
  },
]

function getInitialUsername() {
  if (typeof window === 'undefined') {
    return ''
  }

  return window.localStorage.getItem(LAST_USERNAME_STORAGE_KEY) || ''
}

function getLoginError(error) {
  if (!API) {
    return 'VITE_API_URL non e configurato. Controlla frontend/.env e riavvia Vite.'
  }

  return getApiErrorMessage(error)
}

export default function Login() {
  const [username, setUsername] = useState(getInitialUsername)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [storedUser] = useState(getStoredUser)
  const navigate = useNavigate()

  useEffect(() => {
    if (hasAuthenticatedSession(storedUser)) {
      navigate('/swipe')
    }
  }, [navigate, storedUser])

  async function handleSubmit(event) {
    event.preventDefault()

    const normalizedUsername = username.trim()
    if (!normalizedUsername) {
      return
    }

    setLoading(true)
    setError('')

    try {
      const storedUsername = String(storedUser?.username || '').trim()
      const sameStoredUsername = storedUsername === normalizedUsername
      const authToken = sameStoredUsername ? getStoredAuthToken(storedUser) : ''
      const legacyUserId = sameStoredUsername && !authToken ? getLegacyUserId(storedUser) : null
      const res = await axios.post(`${API}/utenti/login`, {
        username: normalizedUsername,
        auth_token: authToken || null,
        legacy_user_id: legacyUserId,
      })
      storeUserSession(res.data)
      localStorage.setItem(LAST_USERNAME_STORAGE_KEY, normalizedUsername)
      navigate('/swipe')
    } catch (requestError) {
      setError(getLoginError(requestError))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="auth-view auth-view--showcase">
      <section className="auth-card auth-card--showcase">
        <div className="auth-toolbar auth-toolbar--showcase">
          <span className="auth-brand-pill">
            <CompassIcon />
            Livyo
          </span>
          <ThemeToggle compact />
        </div>

        <div className="auth-layout auth-layout--showcase">
          <div className="auth-copy auth-copy--showcase">
            <span className="auth-kicker">Affitti a colpo d'occhio</span>
            <h1 className="auth-title auth-title--showcase">Trova casa senza rumore.</h1>
            <p className="auth-subtitle auth-subtitle--showcase">
              Un ingresso leggero, una UI piu editoriale e un feed costruito per farti decidere
              in pochi secondi.
            </p>

            <div className="auth-pills auth-pills--showcase">
              <span>Swipe mirato</span>
              <span>Servizi vicini</span>
              <span>Preferiti</span>
            </div>

            <div className="auth-preview">
              <div className="auth-preview__surface">
                <div className="auth-preview__topline">
                  <span className="auth-preview__eyebrow">Consigliati per te</span>
                  <span className="auth-preview__link">Vedi tutti</span>
                </div>

                <div className="auth-preview__card">
                  <div className="auth-preview__media">
                    <div className="auth-preview__pill-group">
                      <span className="auth-preview__pill auth-preview__pill--active">Affitto</span>
                      <span className="auth-preview__pill">Arredato</span>
                    </div>
                  </div>

                  <div className="auth-preview__body">
                    <span className="auth-preview__meta">BILOCALE / VOMERO</span>
                    <div className="auth-preview__headline">
                      <strong>Bilocale arredato</strong>
                      <span>EUR 830 / mese</span>
                    </div>
                    <div className="auth-preview__location">
                      <MapPinIcon />
                      Napoli, Via Luca Giordano
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div className="auth-highlight-grid auth-highlight-grid--showcase">
              {TRUST_HIGHLIGHTS.map((item) => {
                const Icon = item.icon

                return (
                  <article key={item.title} className="auth-highlight auth-highlight--showcase">
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
          </div>

          <div className="auth-panel auth-panel--showcase">
            <div className="auth-panel__intro">
              <span className="auth-panel__eyebrow">
                <ShieldIcon />
                Accesso leggero
              </span>
              <h2>Entra con uno username</h2>
              <p>Nessuna password. Ti basta un nome locale per iniziare a salvare annunci.</p>
            </div>

            <form onSubmit={handleSubmit} className="auth-form auth-form--showcase">
              <label className="filter-input auth-input-group" htmlFor="username">
                <span className="filter-section__label">Username</span>
                <input
                  id="username"
                  className="auth-input auth-input--showcase"
                  type="text"
                  placeholder="Scegli un username"
                  value={username}
                  onChange={(event) => setUsername(event.target.value)}
                  maxLength={30}
                  autoComplete="nickname"
                  autoFocus
                  required
                  aria-describedby="auth-helper auth-api-note"
                />
              </label>

              {error && <p className="auth-error auth-error--showcase">{error}</p>}

              <button className="auth-button auth-button--showcase" type="submit" disabled={loading || !username.trim()}>
                {loading ? 'Accesso in corso...' : 'Entra in Livyo'}
              </button>

              <p id="auth-helper" className="auth-helper auth-helper--showcase">
                Se il login non parte, il primo controllo da fare e che il backend sia online.
              </p>

              <p id="auth-api-note" className="auth-api-note">
                API attuale: <code>{getApiDisplayUrl()}</code>
              </p>
            </form>
          </div>
        </div>
      </section>
    </div>
  )
}
