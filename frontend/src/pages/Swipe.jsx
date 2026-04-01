import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'
import AppShell from '../components/AppShell'
import Card from '../components/Card'
import Livyo from '../components/Livyo'
import LocationSheet from '../components/LocationSheet'
import {
  CloseIcon,
  MapPinIcon,
  PdfIcon,
  RefreshIcon,
  SearchIcon,
  TrainIcon,
  TreeIcon,
  UserIcon,
} from '../components/Icons'
import {
  formatMonthlyPrice,
  getListingLocationLine,
  getListingPrimaryArea,
} from '../utils/listings'
import { API_BASE_URL } from '../utils/api'

const API = API_BASE_URL
const FILTERS_STORAGE_KEY = 'casinder-swipe-filters'

const DEFAULT_LOCATION_FILTER = {
  locationId: 'tutta-napoli',
  locationKind: 'all',
  locationLabel: 'Tutta Napoli',
  centerLat: null,
  centerLng: null,
  radiusKm: 5,
}

const DEFAULT_LISTING_FILTERS = {
  tipo: 'casa',
  prezzoMax: '',
  stanzeMin: '',
  superficieMin: '',
  zonaQuery: '',
  soloArredato: false,
  soloAscensore: false,
  soloTerrazzo: false,
  soloPostoAuto: false,
}

const DEFAULT_FILTERS = {
  ...DEFAULT_LISTING_FILTERS,
  ...DEFAULT_LOCATION_FILTER,
}

const FALLBACK_POSITIONS = [
  {
    id: DEFAULT_LOCATION_FILTER.locationId,
    kind: DEFAULT_LOCATION_FILTER.locationKind,
    label: DEFAULT_LOCATION_FILTER.locationLabel,
    listing_count: 0,
  },
]

const FILTER_COUNT_KEYS = [
  'prezzoMax',
  'stanzeMin',
  'superficieMin',
  'zonaQuery',
  'soloArredato',
  'soloAscensore',
  'soloTerrazzo',
  'soloPostoAuto',
]

function getPriceOptions(tipo) {
  if (tipo === 'stanza') {
    return [
      { label: 'Tutte', value: '' },
      { label: `Fino a ${formatMonthlyPrice(200)}`, value: '200' },
      { label: `Fino a ${formatMonthlyPrice(250)}`, value: '250' },
      { label: `Fino a ${formatMonthlyPrice(300)}`, value: '300' },
    ]
  }

  return [
    { label: 'Tutte', value: '' },
    { label: `Fino a ${formatMonthlyPrice(600)}`, value: '600' },
    { label: `Fino a ${formatMonthlyPrice(800)}`, value: '800' },
    { label: `Fino a ${formatMonthlyPrice(1000)}`, value: '1000' },
  ]
}

function getSurfaceOptions(tipo) {
  if (tipo === 'stanza') {
    return [
      { label: 'Qualsiasi', value: '' },
      { label: '15+ mq', value: '15' },
      { label: '20+ mq', value: '20' },
      { label: '25+ mq', value: '25' },
    ]
  }

  return [
    { label: 'Qualsiasi', value: '' },
    { label: '50+ mq', value: '50' },
    { label: '70+ mq', value: '70' },
    { label: '90+ mq', value: '90' },
  ]
}

function normalizeFilters(filters) {
  const locationKind = filters?.locationKind === 'zone' ? 'zone' : 'all'
  const centerLat = locationKind === 'zone' && filters?.centerLat !== null && filters?.centerLat !== ''
    ? Number(filters.centerLat)
    : null
  const centerLng = locationKind === 'zone' && filters?.centerLng !== null && filters?.centerLng !== ''
    ? Number(filters.centerLng)
    : null

  return {
    ...DEFAULT_FILTERS,
    ...filters,
    zonaQuery: String(filters?.zonaQuery || '').trim(),
    locationKind,
    locationLabel: String(filters?.locationLabel || DEFAULT_LOCATION_FILTER.locationLabel).trim()
      || DEFAULT_LOCATION_FILTER.locationLabel,
    centerLat: Number.isFinite(centerLat) ? centerLat : null,
    centerLng: Number.isFinite(centerLng) ? centerLng : null,
    radiusKm: Math.min(20, Math.max(1, Number(filters?.radiusKm || DEFAULT_LOCATION_FILTER.radiusKm))),
  }
}

function getInitialFilters() {
  if (typeof window === 'undefined') {
    return DEFAULT_FILTERS
  }

  try {
    const storedFilters = JSON.parse(window.localStorage.getItem(FILTERS_STORAGE_KEY) || 'null')
    if (!storedFilters || typeof storedFilters !== 'object') {
      return DEFAULT_FILTERS
    }

    return normalizeFilters(storedFilters)
  } catch {
    return DEFAULT_FILTERS
  }
}

function getLocationDraftFromFilters(filters) {
  return {
    locationId: filters.locationId,
    locationKind: filters.locationKind,
    locationLabel: filters.locationLabel,
    centerLat: filters.centerLat,
    centerLng: filters.centerLng,
    radiusKm: filters.radiusKm,
  }
}

function buildListingParams(utenteId, filters) {
  const params = {
    utente_id: utenteId,
    tipo: filters.tipo,
  }

  if (filters.prezzoMax) {
    params.prezzo_max = Number(filters.prezzoMax)
  }

  if (filters.stanzeMin) {
    params.stanze_min = Number(filters.stanzeMin)
  }

  if (filters.superficieMin) {
    params.superficie_min = Number(filters.superficieMin)
  }

  if (filters.zonaQuery.trim()) {
    params.zona_query = filters.zonaQuery.trim()
  }

  if (filters.soloArredato) {
    params.solo_arredato = true
  }

  if (filters.soloAscensore) {
    params.solo_ascensore = true
  }

  if (filters.soloTerrazzo) {
    params.solo_terrazzo = true
  }

  if (filters.soloPostoAuto) {
    params.solo_posto_auto = true
  }

  if (filters.locationKind === 'zone' && filters.centerLat !== null && filters.centerLng !== null) {
    params.center_lat = filters.centerLat
    params.center_lng = filters.centerLng
    params.radius_km = Number(filters.radiusKm)
  }

  return params
}

function countActiveFilters(filters) {
  const listingFilterCount = FILTER_COUNT_KEYS.reduce((count, key) => {
    const value = filters[key]

    if (typeof value === 'boolean') {
      return count + (value ? 1 : 0)
    }

    return count + (String(value || '').trim() ? 1 : 0)
  }, filters.tipo !== DEFAULT_LISTING_FILTERS.tipo ? 1 : 0)

  const locationFilterCount = filters.locationKind === 'zone' && filters.centerLat !== null && filters.centerLng !== null
    ? 1
    : 0

  return listingFilterCount + locationFilterCount
}

function getLocationSummary(filters) {
  if (filters.locationKind === 'zone' && filters.locationLabel) {
    return `Entro ${filters.radiusKm} km da ${filters.locationLabel}`
  }

  return 'Vista estesa su tutta Napoli'
}

function shouldReduceWarmup() {
  if (typeof window === 'undefined') {
    return false
  }

  const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection
  if (connection?.saveData) {
    return true
  }

  if (typeof connection?.effectiveType === 'string' && ['slow-2g', '2g', '3g'].includes(connection.effectiveType)) {
    return true
  }

  return window.matchMedia?.('(max-width: 720px)').matches ?? false
}

function preloadListingImages(listing, limit = 2) {
  const images = Array.isArray(listing?.url_immagini) && listing.url_immagini.length > 0
    ? listing.url_immagini
    : (listing?.url_immagine ? [listing.url_immagine] : [])

  images.slice(0, limit).forEach((imageUrl) => {
    if (!imageUrl) {
      return
    }

    const image = new Image()
    image.src = imageUrl
  })
}

function getShowcaseAreaLabel(annuncio, filters) {
  if (annuncio) {
    return getListingPrimaryArea(annuncio) || filters.locationLabel
  }

  return filters.locationLabel
}

function getShowcaseLocationText(annuncio, filters) {
  if (annuncio) {
    return getListingLocationLine(annuncio)
  }

  if (filters.locationKind === 'zone') {
    return `${filters.locationLabel}, entro ${filters.radiusKm} km`
  }

  return filters.locationLabel
}

function getShowcaseSummary(filters, locationsLoading) {
  if (locationsLoading) {
    return 'Sto preparando le zone piu interessanti per te.'
  }

  if (filters.locationKind === 'zone') {
    return `Feed ristretto entro ${filters.radiusKm} km da ${filters.locationLabel}.`
  }

  return 'Una selezione ordinata per iniziare da subito senza rumore.'
}

function getNearbyServices(areaLabel, filters) {
  return [
    {
      key: 'transport',
      eyebrow: 'Trasporti',
      title: areaLabel || filters.locationLabel,
      time: `${Math.max(3, Number(filters.radiusKm) + 1)} min`,
      Icon: TrainIcon,
    },
    {
      key: 'green',
      eyebrow: 'Verde',
      title: filters.locationKind === 'zone' ? 'Area intorno a te' : 'Spazi aperti',
      time: `${Math.max(6, Number(filters.radiusKm) + 3)} min`,
      Icon: TreeIcon,
    },
  ]
}

function getMarketDelta(annuncio) {
  const seed = Number(annuncio?.id || 42)
  return `+${(((seed % 15) + 35) / 10).toFixed(1)}%`
}

export default function Swipe() {
  const [filters, setFilters] = useState(getInitialFilters)
  const [draftFilters, setDraftFilters] = useState(getInitialFilters)
  const [draftLocation, setDraftLocation] = useState(() => getLocationDraftFromFilters(getInitialFilters()))
  const [annuncio, setAnnuncio] = useState(null)
  const [stato, setStato] = useState('loading')
  const [busy, setBusy] = useState(false)
  const [decisionState, setDecisionState] = useState('')
  const [showFilters, setShowFilters] = useState(false)
  const [showLocationSheet, setShowLocationSheet] = useState(false)
  const [feedback, setFeedback] = useState('')
  const [positions, setPositions] = useState(FALLBACK_POSITIONS)
  const [locationsLoading, setLocationsLoading] = useState(true)
  const [locationsError, setLocationsError] = useState('')
  const [livyoSwipeCount, setLivyoSwipeCount] = useState(0)
  const [livyoSwipeEvent, setLivyoSwipeEvent] = useState(null)
  const [livyoMessageEvent, setLivyoMessageEvent] = useState(null)
  const [toast, setToast] = useState({ visible: false, tone: 'like', message: '' })
  const [reduceWarmup] = useState(() => shouldReduceWarmup())
  const [utente] = useState(() => JSON.parse(localStorage.getItem('utente') || 'null'))
  const toastTimeoutRef = useRef(null)
  const livyoEventIdRef = useRef(0)
  const prefetchedListingRef = useRef(null)
  const requestVersionRef = useRef(0)
  const prefetchRequestRef = useRef(0)
  const prefetchTimerRef = useRef(null)
  const navigate = useNavigate()

  const showToast = useCallback((action) => {
    const toastByAction = {
      like: {
        tone: 'like',
        message: 'Salvato nei preferiti',
      },
      skip: {
        tone: 'skip',
        message: 'Annuncio scartato',
      },
      superlike: {
        tone: 'super',
        message: 'Super like inviato',
      },
    }

    const nextToast = toastByAction[action]
    if (!nextToast) {
      return
    }

    if (toastTimeoutRef.current) {
      window.clearTimeout(toastTimeoutRef.current)
    }

    setToast({
      visible: true,
      tone: nextToast.tone,
      message: nextToast.message,
    })

    toastTimeoutRef.current = window.setTimeout(() => {
      setToast((current) => ({ ...current, visible: false }))
    }, 1500)
  }, [])

  const emitLivyoSwipeEvent = useCallback((action) => {
    livyoEventIdRef.current += 1
    setLivyoSwipeEvent({
      id: livyoEventIdRef.current,
      action,
    })
    setLivyoSwipeCount((current) => current + 1)
  }, [])

  const emitLivyoMessageEvent = useCallback((text) => {
    livyoEventIdRef.current += 1
    setLivyoMessageEvent({
      id: livyoEventIdRef.current,
      text,
    })
  }, [])

  const fetchListing = useCallback(async (filtersToUse, excludeIds = []) => {
    if (!utente) {
      return null
    }

    const params = buildListingParams(utente.id, filtersToUse)
    if (excludeIds.length > 0) {
      params.exclude_ids = excludeIds.join(',')
    }

    const res = await axios.get(`${API}/annunci/prossimo`, { params })
    return res.data
  }, [utente])

  const primeNextListing = useCallback(async (filtersToUse, currentListing, requestVersion) => {
    if (!currentListing || !utente) {
      prefetchedListingRef.current = null
      return
    }

    const prefetchId = prefetchRequestRef.current + 1
    prefetchRequestRef.current = prefetchId

    try {
      const nextListing = await fetchListing(filtersToUse, [currentListing.id])
      if (requestVersionRef.current !== requestVersion || prefetchRequestRef.current !== prefetchId) {
        return
      }

      prefetchedListingRef.current = nextListing
      if (nextListing && !reduceWarmup) {
        preloadListingImages(nextListing, 1)
      }
    } catch {
      if (requestVersionRef.current === requestVersion && prefetchRequestRef.current === prefetchId) {
        prefetchedListingRef.current = null
      }
    }
  }, [fetchListing, reduceWarmup, utente])

  const clearScheduledPrefetch = useCallback(() => {
    if (prefetchTimerRef.current) {
      window.clearTimeout(prefetchTimerRef.current)
      prefetchTimerRef.current = null
    }
  }, [])

  const scheduleNextListingPrefetch = useCallback((filtersToUse, currentListing, requestVersion) => {
    clearScheduledPrefetch()

    if (!currentListing || !utente) {
      prefetchedListingRef.current = null
      return
    }

    const delay = reduceWarmup ? 650 : 0
    if (delay === 0) {
      void primeNextListing(filtersToUse, currentListing, requestVersion)
      return
    }

    prefetchTimerRef.current = window.setTimeout(() => {
      prefetchTimerRef.current = null
      if (requestVersionRef.current !== requestVersion) {
        return
      }

      void primeNextListing(filtersToUse, currentListing, requestVersion)
    }, delay)
  }, [clearScheduledPrefetch, primeNextListing, reduceWarmup, utente])

  const loadCurrentListing = useCallback(async (filtersToUse, { showLoading = true } = {}) => {
    if (!utente) {
      return
    }

    clearScheduledPrefetch()
    const requestVersion = requestVersionRef.current + 1
    requestVersionRef.current = requestVersion
    prefetchRequestRef.current += 1
    prefetchedListingRef.current = null

    if (showLoading) {
      setStato('loading')
    }

    setFeedback('')

    try {
      const nextListing = await fetchListing(filtersToUse)

      if (requestVersionRef.current !== requestVersion) {
        return
      }

      if (!nextListing) {
        setAnnuncio(null)
        setStato('empty')
        return
      }

      if (!reduceWarmup) {
        preloadListingImages(nextListing, 1)
      }

      setAnnuncio(nextListing)
      setStato('ready')
      scheduleNextListingPrefetch(filtersToUse, nextListing, requestVersion)
    } catch {
      if (requestVersionRef.current !== requestVersion) {
        return
      }

      setAnnuncio(null)
      setStato('error')
    }
  }, [clearScheduledPrefetch, fetchListing, reduceWarmup, scheduleNextListingPrefetch, utente])

  const caricaProssimo = useCallback(async (options = {}) => {
    await loadCurrentListing(options.filtersToUse ?? filters, {
      showLoading: options.showLoading ?? true,
    })
  }, [filters, loadCurrentListing])

  const handleDecision = useCallback(async (action) => {
    if (!annuncio || busy) {
      return
    }

    setBusy(true)
    setDecisionState(action)
    setFeedback('')

    try {
      await new Promise((resolve) => setTimeout(resolve, 180))
      await axios.post(`${API}/annunci/${annuncio.id}/${action}`, null, {
        params: { utente_id: utente.id },
      })

      showToast(action)
      emitLivyoSwipeEvent(action)

      const prefetchedListing = prefetchedListingRef.current
      prefetchedListingRef.current = null

      if (prefetchedListing) {
        if (!reduceWarmup) {
          preloadListingImages(prefetchedListing, 1)
        }

        setAnnuncio(prefetchedListing)
        setStato('ready')
        scheduleNextListingPrefetch(filters, prefetchedListing, requestVersionRef.current)
      } else {
        await caricaProssimo({ showLoading: false })
      }
    } catch {
      setFeedback("Non sono riuscito ad aggiornare l'annuncio. Riprova.")
      emitLivyoMessageEvent('Non riesco a salvare questa azione adesso.')
    } finally {
      setDecisionState('')
      setBusy(false)
    }
  }, [
    annuncio,
    busy,
    caricaProssimo,
    emitLivyoMessageEvent,
    emitLivyoSwipeEvent,
    filters,
    reduceWarmup,
    scheduleNextListingPrefetch,
    showToast,
    utente,
  ])

  function openFilters() {
    setDraftFilters(filters)
    setShowFilters(true)
  }

  function closeFilters() {
    setDraftFilters(filters)
    setShowFilters(false)
  }

  function handleApplyFilters() {
    setFilters(normalizeFilters(draftFilters))
    setShowFilters(false)
  }

  function handleResetFilters() {
    setDraftFilters((current) => ({
      ...current,
      ...DEFAULT_LISTING_FILTERS,
    }))
  }

  function updateDraftFilter(key, value) {
    setDraftFilters((current) => ({
      ...current,
      [key]: value,
    }))
  }

  function handleTypeChange(tipo) {
    setDraftFilters((current) => ({
      ...current,
      tipo,
      prezzoMax: '',
      stanzeMin: tipo === 'casa' ? current.stanzeMin : '',
      superficieMin: '',
    }))
  }

  function openLocationSheet() {
    setDraftLocation(getLocationDraftFromFilters(filters))
    setShowLocationSheet(true)
  }

  function closeLocationSheet() {
    setDraftLocation(getLocationDraftFromFilters(filters))
    setShowLocationSheet(false)
  }

  function handlePickLocation(position) {
    setDraftLocation((current) => ({
      ...current,
      locationId: position.id,
      locationKind: position.kind === 'zone' ? 'zone' : 'all',
      locationLabel: position.label,
      centerLat: position.kind === 'zone' ? position.latitude : null,
      centerLng: position.kind === 'zone' ? position.longitude : null,
    }))
  }

  function handleApplyLocation() {
    setFilters((current) => normalizeFilters({
      ...current,
      ...draftLocation,
    }))
    setShowLocationSheet(false)
  }

  function handleResetLocation() {
    setDraftLocation(DEFAULT_LOCATION_FILTER)
  }

  useEffect(() => {
    if (!utente) {
      navigate('/')
      return
    }

    void loadCurrentListing(filters)
  }, [filters, loadCurrentListing, navigate, utente])

  useEffect(() => {
    if (typeof window === 'undefined') {
      return
    }

    window.localStorage.setItem(FILTERS_STORAGE_KEY, JSON.stringify(filters))
  }, [filters])

  useEffect(() => {
    if (!utente) {
      return
    }

    let cancelled = false

    async function loadPositions() {
      setLocationsLoading(true)
      setLocationsError('')

      try {
        const res = await axios.get(`${API}/posizioni`)
        if (cancelled) {
          return
        }

        const nextPositions = Array.isArray(res.data) && res.data.length > 0 ? res.data : FALLBACK_POSITIONS
        setPositions(nextPositions)
      } catch {
        if (!cancelled) {
          setPositions(FALLBACK_POSITIONS)
          setLocationsError('Non sono riuscito a caricare le zone disponibili.')
        }
      } finally {
        if (!cancelled) {
          setLocationsLoading(false)
        }
      }
    }

    void loadPositions()

    return () => {
      cancelled = true
    }
  }, [utente])

  useEffect(() => {
    function handleKeyboardShortcuts(event) {
      if (!annuncio || busy || showFilters || showLocationSheet) {
        return
      }

      const target = event.target
      const tagName = target?.tagName
      if (target?.isContentEditable || tagName === 'INPUT' || tagName === 'TEXTAREA' || tagName === 'SELECT') {
        return
      }

      if (event.key === 'ArrowLeft') {
        event.preventDefault()
        void handleDecision('skip')
      }

      if (event.key === 'ArrowRight') {
        event.preventDefault()
        void handleDecision('like')
      }

      if (event.key === 'ArrowUp') {
        event.preventDefault()
        void handleDecision('superlike')
      }
    }

    window.addEventListener('keydown', handleKeyboardShortcuts)
    return () => window.removeEventListener('keydown', handleKeyboardShortcuts)
  }, [annuncio, busy, handleDecision, showFilters, showLocationSheet])

  useEffect(() => {
    if (!showFilters && !showLocationSheet) {
      return undefined
    }

    function handleEscape(event) {
      if (event.key !== 'Escape') {
        return
      }

      if (showLocationSheet) {
        setDraftLocation(getLocationDraftFromFilters(filters))
        setShowLocationSheet(false)
        return
      }

      if (showFilters) {
        setDraftFilters(filters)
        setShowFilters(false)
      }
    }

    window.addEventListener('keydown', handleEscape)
    return () => window.removeEventListener('keydown', handleEscape)
  }, [filters, showFilters, showLocationSheet])

  useEffect(() => {
    if (stato === 'empty') {
      emitLivyoMessageEvent('Abbiamo finito gli annunci con questi filtri.')
    }

    if (stato === 'error') {
      emitLivyoMessageEvent('La connessione si e presa una pausa.')
    }
  }, [emitLivyoMessageEvent, stato])

  useEffect(() => (
    () => {
      clearScheduledPrefetch()
      if (toastTimeoutRef.current) {
        window.clearTimeout(toastTimeoutRef.current)
      }
    }
  ), [clearScheduledPrefetch])

  const activeFilterCount = useMemo(() => countActiveFilters(filters), [filters])
  const priceOptions = useMemo(() => getPriceOptions(draftFilters.tipo), [draftFilters.tipo])
  const surfaceOptions = useMemo(() => getSurfaceOptions(draftFilters.tipo), [draftFilters.tipo])
  const locationSummary = useMemo(() => getLocationSummary(filters), [filters])
  const showcaseAreaLabel = useMemo(() => getShowcaseAreaLabel(annuncio, filters), [annuncio, filters])
  const showcaseLocationText = useMemo(() => getShowcaseLocationText(annuncio, filters), [annuncio, filters])
  const showcaseSummary = useMemo(() => getShowcaseSummary(filters, locationsLoading), [filters, locationsLoading])
  const nearbyServices = useMemo(
    () => getNearbyServices(showcaseAreaLabel, filters),
    [filters, showcaseAreaLabel],
  )
  const marketDelta = useMemo(() => getMarketDelta(annuncio), [annuncio])
  const avatarLabel = useMemo(
    () => String(utente?.username || '').trim().charAt(0).toUpperCase() || null,
    [utente],
  )

  return (
    <AppShell
      title="Livyo"
      bodyClassName="screen-body--showcase"
      shellClassName="screen-shell--showcase"
      showThemeToggle={false}
      leftSlot={(
        <button
          type="button"
          className={`topbar__action topbar__action--showcase${showFilters ? ' is-active' : ''}`}
          onClick={openFilters}
          aria-label="Apri ricerca"
        >
          <SearchIcon />
          {activeFilterCount > 0 && <span className="topbar__action-badge">{activeFilterCount}</span>}
        </button>
      )}
      rightSlot={(
        <button
          type="button"
          className="topbar__avatar"
          onClick={() => navigate('/profile')}
          aria-label="Vai al profilo"
        >
          <span className="topbar__avatar-ring">
            {avatarLabel ? <span className="topbar__avatar-label">{avatarLabel}</span> : <UserIcon />}
          </span>
        </button>
      )}
    >
      <section className="swipe-showcase">
        {feedback && (
          <p className="inline-message inline-message--error showcase-feedback" role="status">
            {feedback}
          </p>
        )}

        <section className="showcase-panel">
          <header className="showcase-panel__header">
            <div>
              <h1 className="showcase-panel__title">Consigliati per te</h1>
              <p className="showcase-panel__subtitle">{showcaseSummary}</p>
            </div>

            <button type="button" className="showcase-link" onClick={openFilters}>
              Vedi tutti
            </button>
          </header>

          <div className="showcase-panel__chips">
            <button
              type="button"
              className={`showcase-location-chip${filters.locationKind === 'zone' ? ' is-active' : ''}`}
              onClick={openLocationSheet}
            >
              <MapPinIcon />
              <span>{showcaseLocationText}</span>
            </button>

            {activeFilterCount > 0 && (
              <button type="button" className="showcase-filter-chip" onClick={openFilters}>
                {activeFilterCount} filtri
              </button>
            )}
          </div>

          {stato === 'loading' && (
            <div className="showcase-loading-card">
              <div className="showcase-loading-card__media skeleton skeleton--media" />
              <div className="showcase-loading-card__body">
                <div className="skeleton skeleton--title" />
                <div className="skeleton skeleton--price" />
                <div className="skeleton-row">
                  <div className="skeleton skeleton--chip" />
                  <div className="skeleton skeleton--chip" />
                  <div className="skeleton skeleton--chip" />
                </div>
              </div>
            </div>
          )}

          {stato === 'ready' && annuncio && (
            <Card
              key={annuncio.id}
              annuncio={annuncio}
              onLike={() => void handleDecision('like')}
              onSkip={() => void handleDecision('skip')}
              onSuperLike={() => void handleDecision('superlike')}
              busy={busy}
              decision={decisionState}
              prefetchAdjacentImages={!reduceWarmup}
            />
          )}

          {stato === 'empty' && (
            <div className="state-card showcase-state-card">
              <h2>Nessun annuncio con questi filtri</h2>
              <p>Allarga il budget, cambia posizione oppure rimuovi qualche preferenza.</p>
              <button type="button" className="btn btn--primary" onClick={openFilters}>
                Modifica filtri
              </button>
            </div>
          )}

          {stato === 'error' && (
            <div className="state-card showcase-state-card">
              <RefreshIcon className="state-card__icon" />
              <h2>Connessione non disponibile</h2>
              <p>Non sono riuscito a recuperare il prossimo annuncio.</p>
              <button type="button" className="btn btn--primary" onClick={() => void caricaProssimo()}>
                Riprova
              </button>
            </div>
          )}
        </section>

        {stato === 'ready' && annuncio && (
          <>
            <section className="showcase-section-card">
              <header className="showcase-section-card__header">
                <h2 className="showcase-section-card__title">Zona e servizi</h2>
                <button type="button" className="showcase-link showcase-link--with-icon" onClick={openLocationSheet}>
                  Vedi mappa
                  <MapPinIcon />
                </button>
              </header>

              <div className="showcase-map">
                <div className="showcase-map__grid" aria-hidden="true" />
                <div className="showcase-map__badge">
                  <span className="showcase-map__badge-dot" />
                  Servizi nelle vicinanze
                </div>
              </div>

              <div className="showcase-service-grid">
                {nearbyServices.map(({ key, eyebrow, title, time, Icon }) => (
                  <article key={key} className="showcase-service-card">
                    <div className="showcase-service-card__icon">
                      <Icon />
                    </div>

                    <div className="showcase-service-card__copy">
                      <span className="showcase-service-card__eyebrow">{eyebrow}</span>
                      <strong>{title}</strong>
                      <small>{time}</small>
                    </div>
                  </article>
                ))}
              </div>
            </section>

            <section className="showcase-market-card">
              <div className="showcase-market-card__content">
                <div className="showcase-market-card__meta">
                  <span className="showcase-market-card__pill">Mercato</span>
                  <strong>{marketDelta}</strong>
                </div>

                <h2>{showcaseAreaLabel || locationSummary}</h2>
                <p>Trend di zona in crescita nell'ultimo semestre.</p>
              </div>

              <a
                href={annuncio.url}
                target="_blank"
                rel="noreferrer"
                className="showcase-market-card__button"
              >
                <PdfIcon />
                PDF
              </a>
            </section>
          </>
        )}

        {showFilters && (
          <div className="filter-sheet-layer">
            <button
              type="button"
              className="filter-sheet__backdrop"
              aria-label="Chiudi filtri"
              onClick={closeFilters}
            />

            <section className="filter-sheet" role="dialog" aria-modal="true" aria-labelledby="filter-sheet-title">
              <div className="filter-sheet__top">
                <div className="filter-sheet__grabber" aria-hidden="true" />

                <div className="filter-sheet__header">
                  <div className="filter-sheet__heading">
                    <h2 id="filter-sheet-title" className="filter-sheet__title">Filtri</h2>
                    <p className="filter-sheet__subtitle">
                      Personalizza il feed e resta sulle occasioni davvero compatibili.
                    </p>
                  </div>

                  <button
                    type="button"
                    className="topbar__action filter-sheet__close"
                    onClick={closeFilters}
                    aria-label="Chiudi filtri"
                  >
                    <CloseIcon />
                  </button>
                </div>
              </div>

              <div className="filter-sheet__content">
                <section className="filter-section">
                  <p className="filter-section__label">Tipologia</p>
                  <div className="filter-chip-group">
                    <button
                      type="button"
                      className={`filter-chip${draftFilters.tipo === 'casa' ? ' is-active' : ''}`}
                      onClick={() => handleTypeChange('casa')}
                    >
                      Case
                    </button>
                    <button
                      type="button"
                      className={`filter-chip${draftFilters.tipo === 'stanza' ? ' is-active' : ''}`}
                      onClick={() => handleTypeChange('stanza')}
                    >
                      Stanze
                    </button>
                  </div>
                </section>

                <section className="filter-section">
                  <p className="filter-section__label">Budget</p>
                  <div className="filter-option-grid">
                    {priceOptions.map((option) => (
                      <button
                        key={`${draftFilters.tipo}-price-${option.value || 'all'}`}
                        type="button"
                        className={`filter-option${draftFilters.prezzoMax === option.value ? ' is-active' : ''}`}
                        onClick={() => updateDraftFilter('prezzoMax', option.value)}
                      >
                        {option.label}
                      </button>
                    ))}
                  </div>
                </section>

                {draftFilters.tipo === 'casa' && (
                  <section className="filter-section">
                    <p className="filter-section__label">Locali minimi</p>
                    <div className="filter-option-grid">
                      {[
                        { label: 'Qualsiasi', value: '' },
                        { label: '2+', value: '2' },
                        { label: '3+', value: '3' },
                        { label: '4+', value: '4' },
                      ].map((option) => (
                        <button
                          key={`rooms-${option.value || 'all'}`}
                          type="button"
                          className={`filter-option${draftFilters.stanzeMin === option.value ? ' is-active' : ''}`}
                          onClick={() => updateDraftFilter('stanzeMin', option.value)}
                        >
                          {option.label}
                        </button>
                      ))}
                    </div>
                  </section>
                )}

                <section className="filter-section">
                  <p className="filter-section__label">Superficie minima</p>
                  <div className="filter-option-grid">
                    {surfaceOptions.map((option) => (
                      <button
                        key={`${draftFilters.tipo}-surface-${option.value || 'all'}`}
                        type="button"
                        className={`filter-option${draftFilters.superficieMin === option.value ? ' is-active' : ''}`}
                        onClick={() => updateDraftFilter('superficieMin', option.value)}
                      >
                        {option.label}
                      </button>
                    ))}
                  </div>
                </section>

                <section className="filter-section">
                  <label className="filter-input">
                    <span className="filter-section__label">Zona o quartiere</span>
                    <input
                      type="text"
                      className="auth-input filter-text-input"
                      value={draftFilters.zonaQuery}
                      onChange={(event) => updateDraftFilter('zonaQuery', event.target.value)}
                      placeholder="Es. Chiaia, Vomero, Centro"
                      maxLength={40}
                    />
                  </label>
                </section>

                <section className="filter-section">
                  <p className="filter-section__label">Caratteristiche utili</p>
                  <div className="filter-toggle-grid">
                    <button
                      type="button"
                      className={`filter-toggle${draftFilters.soloArredato ? ' is-active' : ''}`}
                      onClick={() => updateDraftFilter('soloArredato', !draftFilters.soloArredato)}
                    >
                      Arredato
                    </button>
                    <button
                      type="button"
                      className={`filter-toggle${draftFilters.soloAscensore ? ' is-active' : ''}`}
                      onClick={() => updateDraftFilter('soloAscensore', !draftFilters.soloAscensore)}
                    >
                      Ascensore
                    </button>
                    <button
                      type="button"
                      className={`filter-toggle${draftFilters.soloTerrazzo ? ' is-active' : ''}`}
                      onClick={() => updateDraftFilter('soloTerrazzo', !draftFilters.soloTerrazzo)}
                    >
                      Terrazzo
                    </button>
                    <button
                      type="button"
                      className={`filter-toggle${draftFilters.soloPostoAuto ? ' is-active' : ''}`}
                      onClick={() => updateDraftFilter('soloPostoAuto', !draftFilters.soloPostoAuto)}
                    >
                      Posto auto
                    </button>
                  </div>
                </section>
              </div>

              <div className="filter-sheet__footer">
                <button type="button" className="btn btn--ghost" onClick={handleResetFilters}>
                  Reset
                </button>
                <button type="button" className="btn btn--primary" onClick={handleApplyFilters}>
                  Applica
                </button>
              </div>
            </section>
          </div>
        )}

        <LocationSheet
          open={showLocationSheet}
          positions={positions}
          loading={locationsLoading}
          error={locationsError}
          draftLocation={draftLocation}
          onPickLocation={handlePickLocation}
          onRadiusChange={(value) => setDraftLocation((current) => ({ ...current, radiusKm: value }))}
          onApply={handleApplyLocation}
          onReset={handleResetLocation}
          onClose={closeLocationSheet}
        />

        <Livyo
          swipeEvent={livyoSwipeEvent}
          messageEvent={livyoMessageEvent}
          swipeCount={livyoSwipeCount}
        />

        <div
          className={`swipe-toast swipe-toast--${toast.tone}${toast.visible ? ' is-visible' : ''}`}
          aria-live="polite"
        >
          {toast.message}
        </div>
      </section>
    </AppShell>
  )
}
