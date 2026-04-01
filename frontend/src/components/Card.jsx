import { useEffect, useRef, useState } from 'react'
import {
  CloseIcon,
  HeartFilledIcon,
  HomeIcon,
  LayoutIcon,
  MapPinIcon,
  RulerIcon,
  ExternalLinkIcon,
  StarIcon,
} from './Icons'
import ImageCarousel from './ImageCarousel'
import {
  formatDistanceKm,
  formatMonthlyPrice,
  getListingDescription,
  getListingImages,
  getListingMeta,
  getListingTitle,
} from '../utils/listings'

const META_ICONS = {
  layout: LayoutIcon,
  location: MapPinIcon,
  surface: RulerIcon,
  contract: HomeIcon,
  source: ExternalLinkIcon,
}

const SWIPE_THRESHOLD_X = 110
const SWIPE_THRESHOLD_Y = 125

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value))
}

function getPointerTargetAction(drag) {
  if (drag.y <= -SWIPE_THRESHOLD_Y && Math.abs(drag.x) < SWIPE_THRESHOLD_X * 0.9) {
    return 'superlike'
  }

  if (drag.x >= SWIPE_THRESHOLD_X) {
    return 'like'
  }

  if (drag.x <= -SWIPE_THRESHOLD_X) {
    return 'skip'
  }

  return ''
}

export default function Card({
  annuncio,
  onLike,
  onSkip,
  onSuperLike,
  busy = false,
  decision = '',
  prefetchAdjacentImages = true,
}) {
  const description = getListingDescription(annuncio)
  const details = getListingMeta(annuncio)
  const images = getListingImages(annuncio)
  const badgeLabel = annuncio.tipo === 'stanza' ? 'Stanza' : 'Casa'
  const distanceLabel = formatDistanceKm(annuncio.distance_km)
  const [drag, setDrag] = useState({ x: 0, y: 0 })
  const [isDragging, setIsDragging] = useState(false)
  const pointerStateRef = useRef({
    pointerId: null,
    startX: 0,
    startY: 0,
    dragging: false,
  })

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      pointerStateRef.current = {
        pointerId: null,
        startX: 0,
        startY: 0,
        dragging: false,
      }
      setDrag({ x: 0, y: 0 })
      setIsDragging(false)
    })

    return () => window.cancelAnimationFrame(frame)
  }, [annuncio?.id])

  useEffect(() => {
    if (decision) {
      return
    }

    const frame = window.requestAnimationFrame(() => {
      pointerStateRef.current = {
        pointerId: null,
        startX: 0,
        startY: 0,
        dragging: false,
      }
      setDrag({ x: 0, y: 0 })
      setIsDragging(false)
    })

    return () => window.cancelAnimationFrame(frame)
  }, [decision])

  const likeProgress = clamp(drag.x / SWIPE_THRESHOLD_X, 0, 1)
  const skipProgress = clamp(-drag.x / SWIPE_THRESHOLD_X, 0, 1)
  const superProgress = clamp(
    (-drag.y / SWIPE_THRESHOLD_Y) * clamp(1 - Math.abs(drag.x) / SWIPE_THRESHOLD_X, 0, 1),
    0,
    1,
  )
  const showDragTransform = !decision && (isDragging || drag.x !== 0 || drag.y !== 0)
  const dragStyle = showDragTransform
    ? {
        transform: `translate3d(${drag.x}px, ${drag.y}px, 0) rotate(${drag.x * 0.035}deg)`,
        transition: isDragging ? 'none' : undefined,
      }
    : undefined

  function requestDecision(nextDecision) {
    if (busy || decision) {
      return
    }

    if (nextDecision === 'like') {
      onLike()
      return
    }

    if (nextDecision === 'skip') {
      onSkip()
      return
    }

    if (nextDecision === 'superlike') {
      onSuperLike()
    }
  }

  function handlePointerDown(event) {
    if (busy || decision || !event.isPrimary) {
      return
    }

    if (event.target.closest('button, a, input, textarea')) {
      return
    }

    pointerStateRef.current = {
      pointerId: event.pointerId,
      startX: event.clientX,
      startY: event.clientY,
      dragging: true,
    }
    setIsDragging(true)
    setDrag({ x: 0, y: 0 })
    event.currentTarget.setPointerCapture(event.pointerId)
  }

  function handlePointerMove(event) {
    const pointerState = pointerStateRef.current
    if (!pointerState.dragging || pointerState.pointerId !== event.pointerId) {
      return
    }

    setDrag({
      x: event.clientX - pointerState.startX,
      y: event.clientY - pointerState.startY,
    })
  }

  function clearPointerState(event) {
    const pointerState = pointerStateRef.current

    if (pointerState.pointerId === event.pointerId && event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId)
    }

    pointerStateRef.current = {
      pointerId: null,
      startX: 0,
      startY: 0,
      dragging: false,
    }
    setIsDragging(false)
  }

  function handlePointerUp(event) {
    const pointerState = pointerStateRef.current
    if (pointerState.pointerId !== event.pointerId) {
      return
    }

    clearPointerState(event)

    const nextAction = getPointerTargetAction(drag)
    if (nextAction) {
      requestDecision(nextAction)
      return
    }

    setDrag({ x: 0, y: 0 })
  }

  function handlePointerCancel(event) {
    const pointerState = pointerStateRef.current
    if (pointerState.pointerId !== event.pointerId) {
      return
    }

    clearPointerState(event)
    setDrag({ x: 0, y: 0 })
  }

  return (
    <section className={`property-scene${decision ? ` is-${decision}` : ''}${isDragging ? ' is-dragging' : ''}`}>
      <div className="property-stack property-stack--back" aria-hidden="true" />
      <div className="property-stack property-stack--mid" aria-hidden="true" />

      <article
        className={`property-card property-card--interactive${isDragging ? ' is-dragging' : ''}`}
        style={dragStyle}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerCancel}
      >
        <span
          className="property-swipe-badge property-swipe-badge--skip"
          style={decision ? undefined : {
            opacity: skipProgress,
            transform: `scale(${0.92 + skipProgress * 0.08}) rotate(-8deg)`,
          }}
        >
          NOPE
        </span>
        <span
          className="property-swipe-badge property-swipe-badge--like"
          style={decision ? undefined : {
            opacity: likeProgress,
            transform: `scale(${0.92 + likeProgress * 0.08}) rotate(8deg)`,
          }}
        >
          LIKE
        </span>
        <span
          className="property-swipe-badge property-swipe-badge--super"
          style={decision ? undefined : {
            opacity: superProgress,
            transform: `translateX(-50%) scale(${0.92 + superProgress * 0.08})`,
          }}
        >
          SUPER
        </span>

        <div className="property-media">
          <div className="property-badge-group">
            <span className="property-badge">{badgeLabel}</span>
            {distanceLabel && <span className="property-badge property-badge--distance">{distanceLabel}</span>}
          </div>
          <ImageCarousel
            key={annuncio.id}
            images={images}
            alt={getListingTitle(annuncio)}
            dragEnabled={false}
            prefetchAdjacent={prefetchAdjacentImages}
          />
        </div>

        <div className="property-content">
          <h2 className="property-title">{getListingTitle(annuncio)}</h2>

          <p className="property-price">
            <span>{formatMonthlyPrice(annuncio.prezzo)}</span>
            <small>/ mese</small>
          </p>

          <div className="property-meta">
            {details.map((detail) => {
              const Icon = META_ICONS[detail.key] || HomeIcon

              return (
                <span key={`${detail.key}-${detail.label}`} className="property-meta__item">
                  <Icon className="property-meta__icon" />
                  {detail.label}
                </span>
              )
            })}
          </div>

          {description && <p className="property-description">{description}</p>}
        </div>
      </article>

      <div className="action-row">
        <button
          type="button"
          className="action-circle action-circle--skip"
          onClick={onSkip}
          disabled={busy}
          aria-label="Scarta annuncio"
        >
          <CloseIcon className="action-circle__icon" />
        </button>

        <button
          type="button"
          className="action-circle action-circle--super"
          onClick={onSuperLike}
          disabled={busy}
          aria-label="Super like"
        >
          <StarIcon className="action-circle__icon" />
        </button>

        <button
          type="button"
          className="action-circle action-circle--like"
          onClick={onLike}
          disabled={busy}
          aria-label="Salva tra i preferiti"
        >
          <HeartFilledIcon className="action-circle__icon" />
        </button>
      </div>
    </section>
  )
}
