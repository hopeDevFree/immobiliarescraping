import { useEffect, useMemo, useRef, useState } from 'react'
import {
  ChevronRightIcon,
  CloseIcon,
  ExternalLinkIcon,
  HeartFilledIcon,
  HomeIcon,
  LayoutIcon,
  MapPinIcon,
  PhotoStackIcon,
  RulerIcon,
  StarIcon,
} from './Icons'
import ImageCarousel from './ImageCarousel'
import {
  formatDistanceKm,
  formatMonthlyPrice,
  getListingImages,
  getListingLayoutLabel,
  getListingLocationLine,
  getListingPrimaryArea,
  getListingSourceLabel,
  getListingSurfaceLabel,
  getListingTitle,
} from '../utils/listings'

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

function getDealLabel(annuncio) {
  return annuncio?.tipo === 'stanza' ? 'Stanza' : 'Affitto'
}

function getEyebrow(annuncio) {
  const area = getListingPrimaryArea(annuncio)
  const layout = getListingLayoutLabel(annuncio)

  return [getDealLabel(annuncio), layout, area]
    .filter(Boolean)
    .map((value) => value.toUpperCase())
    .join(' / ')
}

function getDetailFact(annuncio) {
  const rawText = `${annuncio?.titolo || ''} ${annuncio?.descrizione || ''}`
  const floorMatch = rawText.match(/(\d{1,2})\s*(?:\u00B0|\u00BA)?\s*piano/i)

  if (floorMatch) {
    return {
      label: `${floorMatch[1]}\u00B0 piano`,
      Icon: HomeIcon,
    }
  }

  const distanceLabel = formatDistanceKm(annuncio?.distance_km)
  if (distanceLabel) {
    return {
      label: distanceLabel,
      Icon: MapPinIcon,
    }
  }

  return {
    label: getListingSourceLabel(annuncio?.url),
    Icon: ExternalLinkIcon,
  }
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
  const images = getListingImages(annuncio)
  const locationLine = getListingLocationLine(annuncio)
  const layoutLabel = getListingLayoutLabel(annuncio)
  const surfaceLabel = getListingSurfaceLabel(annuncio) || `${annuncio?.superficie || '--'} mq`
  const roomsLabel = annuncio?.stanze ? `${annuncio.stanze} Locali` : layoutLabel
  const sourceLabel = getListingSourceLabel(annuncio?.url)
  const detailFact = getDetailFact(annuncio)
  const DetailFactIcon = detailFact.Icon
  const photoCount = Math.max(images.length, 1)
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

  const showDragTransform = !decision && (isDragging || drag.x !== 0 || drag.y !== 0)
  const dragStyle = showDragTransform
    ? {
        transform: `translate3d(${drag.x}px, ${drag.y}px, 0) rotate(${drag.x * 0.024}deg)`,
        transition: isDragging ? 'none' : undefined,
      }
    : undefined

  const liftGlowStyle = useMemo(() => {
    const likeProgress = clamp(drag.x / SWIPE_THRESHOLD_X, 0, 1)
    const skipProgress = clamp(-drag.x / SWIPE_THRESHOLD_X, 0, 1)
    const superProgress = clamp(
      (-drag.y / SWIPE_THRESHOLD_Y) * clamp(1 - Math.abs(drag.x) / SWIPE_THRESHOLD_X, 0, 1),
      0,
      1,
    )

    return {
      '--listing-like-progress': likeProgress,
      '--listing-skip-progress': skipProgress,
      '--listing-super-progress': superProgress,
    }
  }, [drag.x, drag.y])

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
    <section
      className={`listing-showcase${decision ? ` is-${decision}` : ''}${isDragging ? ' is-dragging' : ''}`}
      style={liftGlowStyle}
    >
      <article
        className={`listing-showcase__card${isDragging ? ' is-dragging' : ''}`}
        style={dragStyle}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerCancel}
      >
        <div className="listing-showcase__media">
          <div className="listing-showcase__segment">
            <span className="listing-showcase__segment-pill listing-showcase__segment-pill--active">
              {getDealLabel(annuncio)}
            </span>
            <span className="listing-showcase__segment-pill">{sourceLabel}</span>
          </div>

          <ImageCarousel
            key={annuncio.id}
            images={images}
            alt={getListingTitle(annuncio)}
            dragEnabled={false}
            prefetchAdjacent={prefetchAdjacentImages}
            showControls={false}
            showCounter={false}
            showDots={false}
          />

          <span className="listing-showcase__photo-count">
            <PhotoStackIcon />
            {photoCount} foto
          </span>
        </div>

        <div className="listing-showcase__body">
          <p className="listing-showcase__eyebrow">{getEyebrow(annuncio)}</p>

          <div className="listing-showcase__headline">
            <div className="listing-showcase__headline-copy">
              <h2 className="listing-showcase__title">{getListingTitle(annuncio)}</h2>
              <p className="listing-showcase__location">
                <MapPinIcon />
                <span>{locationLine}</span>
              </p>
            </div>

            <p className="listing-showcase__price">{formatMonthlyPrice(annuncio.prezzo)}</p>
          </div>

          <div className="listing-showcase__facts">
            <span className="listing-showcase__fact">
              <RulerIcon />
              {surfaceLabel}
            </span>
            <span className="listing-showcase__fact">
              <LayoutIcon />
              {roomsLabel}
            </span>
            <span className="listing-showcase__fact">
              <DetailFactIcon />
              {detailFact.label}
            </span>
          </div>

          <div className="listing-showcase__actions">
            <button
              type="button"
              className="listing-showcase__action listing-showcase__action--skip"
              onClick={onSkip}
              disabled={busy}
            >
              <CloseIcon />
              Scarta
            </button>

            <button
              type="button"
              className="listing-showcase__action listing-showcase__action--super"
              onClick={onSuperLike}
              disabled={busy}
            >
              <StarIcon />
              Super like
            </button>

            <button
              type="button"
              className="listing-showcase__action listing-showcase__action--like"
              onClick={onLike}
              disabled={busy}
            >
              <HeartFilledIcon />
              Salva
            </button>
          </div>

          <a
            href={annuncio.url}
            target="_blank"
            rel="noreferrer"
            className="listing-showcase__cta"
          >
            <span>Apri su {sourceLabel}</span>
            <ChevronRightIcon />
          </a>
        </div>
      </article>
    </section>
  )
}
