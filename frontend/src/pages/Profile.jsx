import { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'
import AppShell from '../components/AppShell'
import { HeartIcon, HomeIcon, LogOutIcon, UserIcon } from '../components/Icons'
import { formatMonthlyPrice } from '../utils/listings'

const API = import.meta.env.VITE_API_URL
const FILTERS_STORAGE_KEY = 'casinder-swipe-filters'

function readStoredFilters() {
  if (typeof window === 'undefined') {
    return null
  }

  try {
    const parsed = JSON.parse(window.localStorage.getItem(FILTERS_STORAGE_KEY) || 'null')
    return parsed && typeof parsed === 'object' ? parsed : null
  } catch {
    return null
  }
}

function getAveragePrice(listings) {
  const priced = listings
    .map((item) => Number(item.prezzo))
    .filter((price) => Number.isFinite(price))

  if (priced.length === 0) {
    return null
  }

  return Math.round(priced.reduce((sum, price) => sum + price, 0) / priced.length)
}

function getTopLabel(items) {
  if (items.length === 0) {
    return ''
  }

  const counts = items.reduce((map, item) => {
    map.set(item, (map.get(item) || 0) + 1)
    return map
  }, new Map())

  let winner = ''
  let winnerCount = 0

  for (const [label, count] of counts.entries()) {
    if (count > winnerCount) {
      winner = label
      winnerCount = count
    }
  }

  return winner
}

export default function Profile() {
  const navigate = useNavigate()
  const [utente] = useState(() => JSON.parse(localStorage.getItem('utente') || 'null'))
  const [favorites, setFavorites] = useState([])
  const [loading, setLoading] = useState(true)
  const savedFilters = useMemo(readStoredFilters, [])

  useEffect(() => {
    if (!utente) {
      navigate('/')
      return
    }

    async function loadStats() {
      setLoading(true)

      try {
        const res = await axios.get(`${API}/preferiti`, { params: { utente_id: utente.id } })
        setFavorites(res.data)
      } catch {
        setFavorites([])
      } finally {
        setLoading(false)
      }
    }

    void loadStats()
  }, [navigate, utente])

  function handleLogout() {
    localStorage.removeItem('utente')
    navigate('/')
  }

  const profileStats = useMemo(() => {
    const matchesCount = favorites.length
    const superLikesCount = favorites.filter((item) => item.decision === 'superlike').length
    const topZone = getTopLabel(
      favorites
        .map((item) => item.microzona?.trim() || item.macrozona?.trim() || item.zona?.trim())
        .filter(Boolean),
    )

    return {
      matchesCount,
      superLikesCount,
      averagePrice: getAveragePrice(favorites),
      topZone,
    }
  }, [favorites])

  const preferenceLabels = useMemo(() => {
    if (!savedFilters) {
      return []
    }

    const labels = [
      savedFilters.tipo === 'stanza' ? 'Ricerca stanze' : 'Ricerca case',
      savedFilters.locationKind === 'zone' && savedFilters.locationLabel
        ? `Entro ${savedFilters.radiusKm} km da ${savedFilters.locationLabel}`
        : 'Vista estesa su Napoli',
    ]

    if (savedFilters.prezzoMax) {
      labels.push(`Budget fino a ${formatMonthlyPrice(savedFilters.prezzoMax)}`)
    }

    if (savedFilters.soloArredato) {
      labels.push('Solo arredato')
    }

    if (savedFilters.soloTerrazzo) {
      labels.push('Terrazzo')
    }

    if (savedFilters.soloPostoAuto) {
      labels.push('Posto auto')
    }

    return labels
  }, [savedFilters])

  return (
    <AppShell
      title="Profilo"
      leftSlot={(
        <button
          type="button"
          className="topbar__action"
          onClick={() => navigate('/swipe')}
          aria-label="Torna alla schermata di scoperta"
        >
          <HomeIcon />
        </button>
      )}
      rightSlot={(
        <button
          type="button"
          className="topbar__action"
          onClick={() => navigate('/preferiti')}
          aria-label="Vai agli annunci salvati"
        >
          <HeartIcon />
        </button>
      )}
    >
      <section className="page-section">
        <div className="profile-layout">
          <div className="profile-layout__main">
            <div className="profile-hero">
              <div className="profile-hero__icon">
                <UserIcon />
              </div>
              <span className="section-header__eyebrow">Profilo attivo</span>
              <h1>{utente?.username || 'Utente'}</h1>
              <p>
                Qui tieni insieme salvataggi, preferenze di ricerca e ritmo della tua sessione.
              </p>

              {preferenceLabels.length > 0 && (
                <div className="summary-chip-list profile-preferences__list">
                  {preferenceLabels.slice(0, 4).map((label) => (
                    <span key={label} className="summary-chip">
                      {label}
                    </span>
                  ))}
                </div>
              )}
            </div>

            <div className="stats-grid">
              <article className="stat-card">
                <strong>{loading ? '...' : profileStats.matchesCount}</strong>
                <span>match salvati</span>
              </article>
              <article className="stat-card">
                <strong>{loading ? '...' : profileStats.superLikesCount}</strong>
                <span>super like</span>
              </article>
              <article className="stat-card">
                <strong>
                  {loading
                    ? '...'
                    : (profileStats.averagePrice !== null ? formatMonthlyPrice(profileStats.averagePrice) : '-')}
                </strong>
                <span>budget medio</span>
              </article>
              <article className="stat-card">
                <strong>{loading ? '...' : (profileStats.topZone || '-')}</strong>
                <span>zona piu ricorrente</span>
              </article>
            </div>
          </div>

          <div className="profile-actions">
            <span className="floating-panel__label">Sessione</span>
            <p className="profile-note">
              Tema, profilo e ultimi filtri restano nel browser per farti ripartire piu in fretta.
            </p>

            {preferenceLabels.length > 0 && (
              <div className="summary-chip-list profile-preferences__list">
                {preferenceLabels.map((label) => (
                  <span key={label} className="summary-chip">
                    {label}
                  </span>
                ))}
              </div>
            )}

            <button type="button" className="btn btn--primary" onClick={() => navigate('/preferiti')}>
              Vedi i salvati
            </button>
            <button type="button" className="btn btn--ghost" onClick={() => navigate('/swipe')}>
              Torna alla scoperta
            </button>
            <button type="button" className="btn btn--ghost" onClick={handleLogout}>
              <span className="button-icon">
                <LogOutIcon />
                Esci
              </span>
            </button>
          </div>
        </div>
      </section>
    </AppShell>
  )
}
