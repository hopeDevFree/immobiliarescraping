import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'
import AppShell from '../components/AppShell'
import Card from '../components/Card'
import Livyo from '../components/Livyo'
import LocationSheet from '../components/LocationSheet'
import {
  CloseIcon,
  ExternalLinkIcon,
  MapPinIcon,
  RefreshIcon,
  SlidersIcon,
  UserIcon,
} from '../components/Icons'
import {
  formatMonthlyPrice,
  getListingDescription,
  getListingMeta,
  getListingSourceLabel,
  getListingTitle,
} from '../utils/listings'

const API = import.meta.env.VITE_API_URL
const FILTERS_STORAGE_KEY = 'casinder-swipe-filters'
const SWIPE_SHORTCUTS_HINT = '\u2190 scarta | \u2191 super like | \u2192 salva'

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

function getSearchHeadline(filters) {
  if (filters.tipo === 'stanza') {
    return 'Trova stanze che meritano davvero attenzione'
  }

  return 'Scopri case interessanti senza perderti nel rumore'
}

function getSearchSupportText(filters) {
  if (filters.locationKind === 'zone') {
    return `Stai guardando ${filters.tipo === 'stanza' ? 'stanze' : 'case'} entro ${filters.radiusKm} km da ${filters.locationLabel}.`
  }

  return `Stai esplorando ${filters.tipo === 'stanza' ? 'stanze' : 'case'} in tutta Napoli con un flusso piu rapido e leggibile.`
}

function getAppliedFilterLabels(filters) {
  const labels = [filters.tipo === 'stanza' ? 'Modalita stanze' : 'Modalita case']

  if (filters.locationKind === 'zone') {
    labels.push(`Raggio ${filters.radiusKm} km`)
  } else {
    labels.push('Area ampia')
  }

  if (filters.prezzoMax) {
    labels.push(`Budget ${formatMonthlyPrice(filters.prezzoMax)}`)
  }

  if (filters.stanzeMin) {
    labels.push(`${filters.stanzeMin}+ locali`)
  }

  if (filters.superficieMin) {
    labels.push(`${filters.superficieMin}+ mq`)
  }

  if (filters.zonaQuery.trim()) {
    labels.push(`Zona ${filters.zonaQuery.trim()}`)
  }

  if (filters.soloArredato) {
    labels.push('Arredato')
  }

  if (filters.soloAscensore) {
    labels.push('Ascensore')
  }

  if (filters.soloTerrazzo) {
    labels.push('Terrazzo')
  }

  if (filters.soloPostoAuto) {
    labels.push('Posto auto')
  }

  return labels
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

function truncateText(text, maxLength = 140) {
  const normalized = String(text || '').trim()
  if (!normalized) {
    return ''
  }

  if (normalized.length <= maxLength) {
    return normalized
  }

  return `${normalized.slice(0, maxLength - 3).trim()}...`
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
  const searchHeadline = useMemo(() => getSearchHeadline(filters), [filters])
  const searchSupportText = useMemo(() => getSearchSupportText(filters), [filters])
  const activeFilterLabels = useMemo(() => getAppliedFilterLabels(filters), [filters])
  const spotlightTitle = useMemo(() => (annuncio ? getListingTitle(annuncio) : ''), [annuncio])
  const spotlightDescription = useMemo(() => {
    if (!annuncio) {
      return ''
    }

    const description = getListingDescription(annuncio)
    if (description) {
      return truncateText(description, 150)
    }

    return `${formatMonthlyPrice(annuncio.prezzo)} al mese su ${getListingSourceLabel(annuncio.url)}.`
  }, [annuncio])
  const spotlightChips = useMemo(() => {
    if (!annuncio) {
      return []
    }

    const chips = [...getListingMeta(annuncio).map((item) => item.label)]
    const sourceLabel = getListingSourceLabel(annuncio.url)

    if (sourceLabel && !chips.includes(sourceLabel)) {
      chips.push(sourceLabel)
    }

    return chips.slice(0, 4)
  }, [annuncio])

  return (
    <AppShell
      title="Scopri"
      bodyClassName="screen-body--swipe"
      leftSlot={(
        <button
          type="button"
          className={`topbar__action${showFilters ? ' is-active' : ''}`}
          onClick={openFilters}
          aria-label="Apri filtri"
        >
          <SlidersIcon />
          {activeFilterCount > 0 && <span className="topbar__action-badge">{activeFilterCount}</span>}
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
      <section className="swipe-screen">
        {feedback && (
          <p className="inline-message inline-message--error inline-message--floating" role="status">
            {feedback}
          </p>
        )}

        <aside className="swipe-screen__sidebar">
          <div className="hero-copy">
            <span className="hero-copy__eyebrow">Ricerca attiva</span>
            <h1 className="swipe-hero__title">{searchHeadline}</h1>
            <p className="hero-copy__text">{searchSupportText}</p>
          </div>

          <div className="floating-panel swipe-panel">
            <span className="floating-panel__label">Setup veloce</span>

            <div className="swipe-insight-grid">
              <article className="swipe-insight">
                <strong>{filters.tipo === 'stanza' ? 'Stanze' : 'Case'}</strong>
                <span>modalita attiva</span>
              </article>
              <article className="swipe-insight">
                <strong>{filters.locationKind === 'zone' ? `${filters.radiusKm} km` : 'Ampia'}</strong>
                <span>copertura</span>
              </article>
              <article className="swipe-insight">
                <strong>{activeFilterCount}</strong>
                <span>filtri extra</span>
              </article>
            </div>

            <div className="summary-chip-list">
              {activeFilterLabels.slice(0, 6).map((label) => (
                <span key={label} className="summary-chip">
                  {label}
                </span>
              ))}
            </div>
          </div>

          {stato === 'ready' && annuncio ? (
            <div className="floating-panel listing-spotlight">
              <span className="floating-panel__label">Annuncio in focus</span>
              <h2 className="listing-spotlight__title">{spotlightTitle}</h2>
              <p className="listing-spotlight__copy">{spotlightDescription}</p>

              <div className="summary-chip-list">
                {spotlightChips.map((chip) => (
                  <span key={chip} className="summary-chip">
                    {chip}
                  </span>
                ))}
              </div>

              <div className="spotlight-actions">
                <a
                  href={annuncio.url}
                  target="_blank"
                  rel="noreferrer"
                  className="btn btn--primary"
                >
                  <span className="button-icon">
                    <ExternalLinkIcon />
                    Apri fonte
                  </span>
                </a>
              </div>
            </div>
          ) : (
            <div className="floating-panel swipe-panel">
              <span className="floating-panel__label">Come usarlo</span>
              <p className="swipe-panel__text">
                Scorri veloce quando un annuncio non fa per te, salva quando vale la pena,
                e usa i filtri per dare ritmo alla ricerca.
              </p>
              <div className="summary-chip-list">
                <span className="summary-chip">Filtri persistenti</span>
                <span className="summary-chip">Scorciatoie tastiera</span>
                <span className="summary-chip">Feedback immediato</span>
              </div>
            </div>
          )}
        </aside>

        <div className="swipe-screen__main">
          <button
            type="button"
            className={`location-pill${filters.locationKind === 'zone' ? ' is-active' : ''}`}
            onClick={openLocationSheet}
          >
            <span className="location-pill__icon">
              <MapPinIcon />
            </span>
            <span className="location-pill__copy">
              <strong>{filters.locationLabel}</strong>
              <small>{locationsLoading ? 'Carico le zone disponibili...' : locationSummary}</small>
            </span>
          </button>

          {stato === 'loading' && (
            <div className="property-scene">
              <div className="property-stack property-stack--back" aria-hidden="true" />
              <div className="property-stack property-stack--mid" aria-hidden="true" />
              <div className="property-card property-card--loading">
                <div className="skeleton skeleton--media" />
                <div className="property-content">
                  <div className="skeleton skeleton--title" />
                  <div className="skeleton skeleton--price" />
                  <div className="skeleton-row">
                    <div className="skeleton skeleton--chip" />
                    <div className="skeleton skeleton--chip" />
                    <div className="skeleton skeleton--chip" />
                  </div>
                </div>
              </div>
            </div>
          )}

          {stato === 'ready' && annuncio && (
            <>
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
              <p className="swipe-shortcuts">{SWIPE_SHORTCUTS_HINT}</p>
            </>
          )}

          {stato === 'empty' && (
            <div className="state-card">
              <h2>Nessun annuncio con questi filtri</h2>
              <p>Allarga il budget, cambia posizione oppure rimuovi qualche preferenza.</p>
              <button type="button" className="btn btn--primary" onClick={openFilters}>
                Modifica filtri
              </button>
            </div>
          )}

          {stato === 'error' && (
            <div className="state-card">
              <RefreshIcon className="state-card__icon" />
              <h2>Connessione non disponibile</h2>
              <p>Non sono riuscito a recuperare il prossimo annuncio.</p>
              <button type="button" className="btn btn--primary" onClick={() => void caricaProssimo()}>
                Riprova
              </button>
            </div>
          )}
        </div>

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
