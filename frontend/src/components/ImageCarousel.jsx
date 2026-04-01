import { useEffect, useMemo, useRef, useState } from 'react'
import { ChevronLeftIcon, ChevronRightIcon } from './Icons'

export default function ImageCarousel({ images, alt, compact = false, dragEnabled = true, prefetchAdjacent = true }) {
  const gallery = useMemo(() => (Array.isArray(images) ? images.filter(Boolean) : []), [images])
  const [currentIndex, setCurrentIndex] = useState(0)
  const [loadedImages, setLoadedImages] = useState(() => new Set())
  const [dragOffset, setDragOffset] = useState(0)
  const [isDragging, setIsDragging] = useState(false)
  const pointerStateRef = useRef({
    pointerId: null,
    startX: 0,
    deltaX: 0,
    dragging: false,
  })
  const hasMultipleImages = gallery.length > 1
  const activeIndex = gallery.length > 0 ? Math.min(currentIndex, gallery.length - 1) : 0

  useEffect(() => {
    if (!prefetchAdjacent || !gallery[activeIndex]) {
      return
    }

    const indexesToWarm = [activeIndex - 1, activeIndex, activeIndex + 1]
      .map((index) => (index + gallery.length) % gallery.length)
      .filter((index, position, array) => array.indexOf(index) === position)

    indexesToWarm.forEach((index) => {
      const src = gallery[index]
      if (!src) {
        return
      }

      const image = new Image()
      image.src = src
    })
  }, [activeIndex, gallery, prefetchAdjacent])

  if (gallery.length === 0) {
    return <div className={`carousel carousel--empty${compact ? ' carousel--compact' : ''}`} aria-hidden="true" />
  }

  function showPreviousImage() {
    setDragOffset(0)
    setCurrentIndex((index) => (index === 0 ? gallery.length - 1 : index - 1))
  }

  function showNextImage() {
    setDragOffset(0)
    setCurrentIndex((index) => (index === gallery.length - 1 ? 0 : index + 1))
  }

  function handleImageLoad(index) {
    setLoadedImages((current) => {
      if (current.has(index)) {
        return current
      }

      const next = new Set(current)
      next.add(index)
      return next
    })
  }

  function handlePointerDown(event) {
    if (!dragEnabled || !hasMultipleImages || !event.isPrimary) {
      return
    }

    pointerStateRef.current = {
      pointerId: event.pointerId,
      startX: event.clientX,
      deltaX: 0,
      dragging: true,
    }
    setIsDragging(true)
    setDragOffset(0)
    event.currentTarget.setPointerCapture(event.pointerId)
  }

  function handlePointerMove(event) {
    if (!dragEnabled) {
      return
    }

    const pointerState = pointerStateRef.current
    if (!pointerState.dragging || pointerState.pointerId !== event.pointerId) {
      return
    }

    const deltaX = event.clientX - pointerState.startX
    pointerState.deltaX = deltaX
    setDragOffset(deltaX)
  }

  function handlePointerUp(event) {
    if (!dragEnabled) {
      return
    }

    const pointerState = pointerStateRef.current
    if (pointerState.pointerId !== event.pointerId) {
      return
    }

    const threshold = 48
    const travelled = pointerState.deltaX
    pointerStateRef.current = {
      pointerId: null,
      startX: 0,
      deltaX: 0,
      dragging: false,
    }

    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId)
    }

    setDragOffset(0)
    setIsDragging(false)

    if (travelled <= -threshold) {
      showNextImage()
      return
    }

    if (travelled >= threshold) {
      showPreviousImage()
    }
  }

  function handlePointerCancel(event) {
    if (!dragEnabled) {
      return
    }

    const pointerState = pointerStateRef.current
    if (pointerState.pointerId !== event.pointerId) {
      return
    }

    pointerStateRef.current = {
      pointerId: null,
      startX: 0,
      deltaX: 0,
      dragging: false,
    }

    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId)
    }

    setDragOffset(0)
    setIsDragging(false)
  }

  function handleKeyDown(event) {
    if (!hasMultipleImages) {
      return
    }

    if (event.key === 'ArrowLeft') {
      event.preventDefault()
      showPreviousImage()
    }

    if (event.key === 'ArrowRight') {
      event.preventDefault()
      showNextImage()
    }
  }

  const translatePercentage = `calc(${-activeIndex * 100}% + ${dragOffset}px)`

  return (
    <div
      className={`carousel${compact ? ' carousel--compact' : ''}`}
      role="region"
      aria-label={`Galleria immagini di ${alt}`}
    >
      <div
        className={`carousel__viewport${isDragging ? ' is-dragging' : ''}${!dragEnabled ? ' is-static' : ''}`}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerCancel}
        onKeyDown={handleKeyDown}
        tabIndex={hasMultipleImages ? 0 : -1}
      >
        <div className="carousel__track" style={{ transform: `translate3d(${translatePercentage}, 0, 0)` }}>
          {gallery.map((imageUrl, index) => (
            <div key={`${imageUrl}-${index}`} className="carousel__slide">
              <div className={`carousel__image-shell${loadedImages.has(index) ? ' is-loaded' : ''}`}>
                <img
                  src={imageUrl}
                  alt={`${alt} - immagine ${index + 1}`}
                  className="carousel__image"
                  draggable="false"
                  loading={index === 0 ? 'eager' : 'lazy'}
                  onLoad={() => handleImageLoad(index)}
                />
              </div>
            </div>
          ))}
        </div>
      </div>

      {hasMultipleImages && (
        <>
          <button
            type="button"
            className="carousel__control carousel__control--prev"
            onClick={showPreviousImage}
            aria-label="Immagine precedente"
          >
            <ChevronLeftIcon />
          </button>

          <button
            type="button"
            className="carousel__control carousel__control--next"
            onClick={showNextImage}
            aria-label="Immagine successiva"
          >
            <ChevronRightIcon />
          </button>

          <span className="carousel__counter">
            {activeIndex + 1} / {gallery.length}
          </span>

          <div className="carousel__dots" aria-hidden="true">
            {gallery.map((imageUrl, index) => (
              <span
                key={`${imageUrl}-${index}`}
                className={`carousel__dot${index === activeIndex ? ' is-active' : ''}`}
              />
            ))}
          </div>
        </>
      )}
    </div>
  )
}
