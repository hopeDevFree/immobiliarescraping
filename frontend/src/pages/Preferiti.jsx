import { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'
import AppShell from '../components/AppShell'
import ImageCarousel from '../components/ImageCarousel'
import { HomeIcon, RefreshIcon, UserIcon } from '../components/Icons'
import {
  formatMonthlyPrice,
  getListingDescription,
  getListingImages,
  getListingMeta,
  getListingSourceLabel,
  getListingTitle,
} from '../utils/listings'
import { API_BASE_URL, getApiErrorMessage } from '../utils/api'

const API = API_BASE_URL

function getAveragePrice(listings) {
  const pricedListings = listings
    .map((item) => Number(item.prezzo))
    .filter((price) => Number.isFinite(price))

  if (pricedListings.length === 0) {
    return null
  }

  const total = pricedListings.reduce((sum, price) => sum + price, 0)
  return Math.round(total / pricedListings.length)
}

export default function Preferiti() {
  const [preferiti, setPreferiti] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [busyId, setBusyId] = useState(null)
  const [filterMode, setFilterMode] = useState('all')
  const [sortMode, setSortMode] = useState('feed')
  const navigate = useNavigate()
  const [utente] = useState(() => JSON.parse(localStorage.getItem('utente') || 'null'))

  useEffect(() => {
    if (!utente) {
      navigate('/')
      return
    }

    async function loadPreferiti() {
      setLoading(true)
      setError('')

      try {
        const res = await axios.get(`${API}/preferiti`, { params: { utente_id: utente.id } })
        setPreferiti(res.data)
      } catch (error) {
        setError(getApiErrorMessage(error))
      } finally {
        setLoading(false)
      }
    }

    void loadPreferiti()
  }, [navigate, utente])

  async function rimuovi(id) {
    setBusyId(id)

    try {
      await axios.delete(`${API}/preferiti/${id}`, { params: { utente_id: utente.id } })
      setPreferiti((current) => current.filter((preferito) => preferito.id !== id))
    } catch (error) {
      setError(getApiErrorMessage(error))
    } finally {
      setBusyId(null)
    }
  }

  const stats = useMemo(() => ({
    total: preferiti.length,
    superLikes: preferiti.filter((item) => item.decision === 'superlike').length,
    averagePrice: getAveragePrice(preferiti),
  }), [preferiti])

  const visiblePreferiti = useMemo(() => {
    const filtered = filterMode === 'superlike'
      ? preferiti.filter((item) => item.decision === 'superlike')
      : [...preferiti]

    if (sortMode === 'price-asc') {
      filtered.sort((left, right) => Number(left.prezzo || 0) - Number(right.prezzo || 0))
    }

    if (sortMode === 'price-desc') {
      filtered.sort((left, right) => Number(right.prezzo || 0) - Number(left.prezzo || 0))
    }

    return filtered
  }, [filterMode, preferiti, sortMode])

  return (
    <AppShell
      title="Salvati"
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
          onClick={() => navigate('/profile')}
          aria-label="Vai al profilo"
        >
          <UserIcon />
        </button>
      )}
    >
      <section className="page-section">
        <header className="section-header">
          <div>
            <span className="section-header__eyebrow">I tuoi match</span>
            <h1 className="section-header__title">Annunci da riguardare</h1>
            <p className="section-header__text">
              Qui rimane solo quello che merita un secondo sguardo.
            </p>
          </div>
          <span className="section-badge">{stats.total}</span>
        </header>

        <div className="stats-grid">
          <article className="stat-card">
            <strong>{stats.total}</strong>
            <span>annunci salvati</span>
          </article>
          <article className="stat-card">
            <strong>{stats.superLikes}</strong>
            <span>super like</span>
          </article>
          <article className="stat-card">
            <strong>{stats.averagePrice !== null ? formatMonthlyPrice(stats.averagePrice) : '-'}</strong>
            <span>budget medio</span>
          </article>
        </div>

        <div className="utility-toolbar">
          <div className="filter-chip-group filter-chip-group--compact">
            <button
              type="button"
              className={`filter-chip${filterMode === 'all' ? ' is-active' : ''}`}
              onClick={() => setFilterMode('all')}
              aria-pressed={filterMode === 'all'}
            >
              Tutti
            </button>
            <button
              type="button"
              className={`filter-chip${filterMode === 'superlike' ? ' is-active' : ''}`}
              onClick={() => setFilterMode('superlike')}
              aria-pressed={filterMode === 'superlike'}
            >
              Solo super like
            </button>
          </div>

          <div className="utility-toolbar__group">
            <button
              type="button"
              className={`filter-option${sortMode === 'feed' ? ' is-active' : ''}`}
              onClick={() => setSortMode('feed')}
              aria-pressed={sortMode === 'feed'}
            >
              Ordine feed
            </button>
            <button
              type="button"
              className={`filter-option${sortMode === 'price-asc' ? ' is-active' : ''}`}
              onClick={() => setSortMode('price-asc')}
              aria-pressed={sortMode === 'price-asc'}
            >
              Prezzo crescente
            </button>
            <button
              type="button"
              className={`filter-option${sortMode === 'price-desc' ? ' is-active' : ''}`}
              onClick={() => setSortMode('price-desc')}
              aria-pressed={sortMode === 'price-desc'}
            >
              Prezzo alto
            </button>
          </div>
        </div>

        {error && <p className="inline-message inline-message--error">{error}</p>}

        {loading && (
          <div className="state-card">
            <RefreshIcon className="state-card__icon" />
            <h2>Caricamento preferiti</h2>
            <p>Sto recuperando gli annunci che hai salvato.</p>
          </div>
        )}

        {!loading && preferiti.length === 0 && !error && (
          <div className="state-card">
            <h2>Nessun preferito ancora</h2>
            <p>Inizia dallo swipe e salva gli annunci che meritano un secondo sguardo.</p>
            <button type="button" className="btn btn--primary" onClick={() => navigate('/swipe')}>
              Vai alla scoperta
            </button>
          </div>
        )}

        {!loading && preferiti.length > 0 && visiblePreferiti.length === 0 && (
          <div className="state-card">
            <h2>Nessun risultato con questo filtro</h2>
            <p>Prova a mostrare di nuovo tutti gli annunci oppure cambia ordinamento.</p>
            <button type="button" className="btn btn--primary" onClick={() => setFilterMode('all')}>
              Mostra tutto
            </button>
          </div>
        )}

        <div className="match-list">
          {visiblePreferiti.map((annuncio) => {
            const description = getListingDescription(annuncio)
            const sourceLabel = getListingSourceLabel(annuncio.url)

            return (
              <article key={annuncio.id} className="match-card">
                <ImageCarousel
                  images={getListingImages(annuncio)}
                  alt={getListingTitle(annuncio)}
                  compact
                />

                <div className="match-card__body">
                  <div className="match-card__head">
                    <div>
                      <h2 className="match-card__title">{getListingTitle(annuncio)}</h2>
                      <p className="match-card__price">{formatMonthlyPrice(annuncio.prezzo)} / mese</p>
                    </div>
                    <div className="match-card__tags">
                      <span className="match-card__tag">{annuncio.tipo === 'stanza' ? 'Stanza' : 'Casa'}</span>
                      <span className="match-card__tag">{sourceLabel}</span>
                      {annuncio.decision === 'superlike' && (
                        <span className="match-card__tag match-card__tag--super">
                          Super like
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="match-card__meta">
                    {getListingMeta(annuncio).slice(0, 3).map((detail) => (
                      <span key={`${annuncio.id}-${detail.key}-${detail.label}`} className="match-chip">
                        {detail.label}
                      </span>
                    ))}
                  </div>

                  {description && <p className="match-card__description">{description}</p>}

                  <div className="match-card__actions">
                    <a
                      href={annuncio.url}
                      target="_blank"
                      rel="noreferrer"
                      className="btn btn--primary"
                    >
                      Apri annuncio originale
                    </a>
                    <button
                      type="button"
                      className="btn btn--ghost"
                      onClick={() => rimuovi(annuncio.id)}
                      disabled={busyId === annuncio.id}
                    >
                      {busyId === annuncio.id ? 'Rimozione...' : 'Rimuovi'}
                    </button>
                  </div>
                </div>
              </article>
            )
          })}
        </div>
      </section>
    </AppShell>
  )
}
