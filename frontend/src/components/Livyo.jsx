import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

const BUBBLE_TIMEOUT_MS = 2800
const PARTICLE_TIMEOUT_MS = 700
const RIPPLE_TIMEOUT_MS = 900

const TONE_BY_ACTION = {
  like: 'like',
  skip: 'skip',
  superlike: 'super',
}

const PALETTE = {
  calm: {
    base: '#4F46E5',
    accent: '#7C7BFF',
    shadow: 'rgba(79, 70, 229, 0.28)',
    particle: '#A5B4FC',
  },
  like: {
    base: '#FD267A',
    accent: '#FF7A5C',
    shadow: 'rgba(253, 38, 122, 0.24)',
    particle: '#FDB7D2',
  },
  skip: {
    base: '#FF5A66',
    accent: '#FF8F8F',
    shadow: 'rgba(255, 90, 102, 0.22)',
    particle: '#FFC2C6',
  },
  super: {
    base: '#6D5DF6',
    accent: '#4CB8FF',
    shadow: 'rgba(109, 93, 246, 0.22)',
    particle: '#C7C8FF',
  },
  error: {
    base: '#2563EB',
    accent: '#60A5FA',
    shadow: 'rgba(37, 99, 235, 0.2)',
    particle: '#BFDBFE',
  },
}

const ACTION_MESSAGES = {
  like: [
    'Questo merita un secondo sguardo.',
    'Bel colpo: salvato al momento giusto.',
    'Qui c e materiale interessante.',
  ],
  skip: [
    'Via, senza rimpianti.',
    'Meglio tenere il feed pulito.',
    'Passiamo al prossimo con fiducia.',
  ],
  superlike: [
    'Qui hai visto del vero potenziale.',
    'Super like ben speso.',
    'Segnato come occasione da monitorare.',
  ],
}

const MANUAL_TIPS = [
  'Usa il raggio per stringere il feed quando hai gia una zona chiara.',
  'Con un budget massimo il feed diventa molto piu leggibile.',
  'Salva solo quello che apriresti davvero una seconda volta.',
]

function randomItem(items) {
  return items[Math.floor(Math.random() * items.length)]
}

function buildParticles(tone) {
  return Array.from({ length: 6 }, (_, index) => ({
    id: `${tone}-${Date.now()}-${index}`,
    tone,
    size: 6 + (index % 3) * 3,
    tx: `${-24 + index * 9}px`,
    ty: `${-18 - (index % 3) * 10}px`,
    delay: `${index * 0.03}s`,
  }))
}

export default function Livyo({ swipeEvent, messageEvent, swipeCount = 0 }) {
  const [bubble, setBubble] = useState('Pronto a leggere il feed con te.')
  const [particles, setParticles] = useState([])
  const [ripples, setRipples] = useState([])
  const tipIndexRef = useRef(0)
  const bubbleTimeoutRef = useRef(null)
  const particleTimeoutRef = useRef(null)
  const rippleTimeoutRef = useRef(null)
  const mood = useMemo(() => {
    const latestMessageId = messageEvent?.id || -1
    const latestSwipeId = swipeEvent?.id || -1

    if (latestMessageId > latestSwipeId) {
      return 'error'
    }

    if (latestSwipeId > 0) {
      return TONE_BY_ACTION[swipeEvent.action] || 'calm'
    }

    return 'calm'
  }, [messageEvent, swipeEvent])
  const palette = useMemo(() => PALETTE[mood] || PALETTE.calm, [mood])

  const showBubble = useCallback((text) => {
    if (bubbleTimeoutRef.current) {
      window.clearTimeout(bubbleTimeoutRef.current)
    }

    setBubble(text)
    bubbleTimeoutRef.current = window.setTimeout(() => {
      setBubble('')
    }, BUBBLE_TIMEOUT_MS)
  }, [])

  const pulse = useCallback((tone) => {
    setRipples([{ id: `${tone}-${Date.now()}`, tone }])
    setParticles(buildParticles(tone))

    if (particleTimeoutRef.current) {
      window.clearTimeout(particleTimeoutRef.current)
    }

    if (rippleTimeoutRef.current) {
      window.clearTimeout(rippleTimeoutRef.current)
    }

    particleTimeoutRef.current = window.setTimeout(() => {
      setParticles([])
    }, PARTICLE_TIMEOUT_MS)

    rippleTimeoutRef.current = window.setTimeout(() => {
      setRipples([])
    }, RIPPLE_TIMEOUT_MS)
  }, [])

  useEffect(() => {
    if (!swipeEvent?.id) {
      return
    }

    const nextMood = TONE_BY_ACTION[swipeEvent.action] || 'calm'
    const timeoutId = window.setTimeout(() => {
      showBubble(randomItem(ACTION_MESSAGES[swipeEvent.action] || ACTION_MESSAGES.like))
      pulse(nextMood)
    }, 0)

    return () => window.clearTimeout(timeoutId)
  }, [pulse, showBubble, swipeEvent])

  useEffect(() => {
    if (!messageEvent?.id || !messageEvent.text) {
      return
    }

    const timeoutId = window.setTimeout(() => {
      showBubble(messageEvent.text)
      pulse('error')
    }, 0)

    return () => window.clearTimeout(timeoutId)
  }, [messageEvent, pulse, showBubble])

  useEffect(() => {
    if (swipeCount > 0 && swipeCount % 5 === 0) {
      const timeoutId = window.setTimeout(() => {
        showBubble(`Hai gia valutato ${swipeCount} annunci: il feed si sta affinando.`)
        pulse('calm')
      }, 0)

      return () => window.clearTimeout(timeoutId)
    }

    return undefined
  }, [pulse, showBubble, swipeCount])

  useEffect(() => (
    () => {
      if (bubbleTimeoutRef.current) {
        window.clearTimeout(bubbleTimeoutRef.current)
      }

      if (particleTimeoutRef.current) {
        window.clearTimeout(particleTimeoutRef.current)
      }

      if (rippleTimeoutRef.current) {
        window.clearTimeout(rippleTimeoutRef.current)
      }
    }
  ), [])

  function handleManualTip() {
    tipIndexRef.current = (tipIndexRef.current + 1) % MANUAL_TIPS.length
    showBubble(MANUAL_TIPS[tipIndexRef.current])
    pulse('calm')
  }

  return (
    <div className="livyo" aria-live="polite">
      {bubble && (
        <div className="livyo__bubble" role="status">
          {bubble}
        </div>
      )}

      <div className="livyo__stage">
        <span className="livyo__shadow" style={{ background: palette.shadow }} />

        {ripples.map((ripple) => (
          <svg key={ripple.id} className="livyo__svg livyo__stage-layer" viewBox="0 0 64 64" aria-hidden="true">
            <circle className="livyo__ripple" cx="36" cy="35" r="17" stroke={PALETTE[ripple.tone].accent} />
          </svg>
        ))}

        {particles.map((particle) => (
          <span
            key={particle.id}
            className="livyo__particle"
            style={{
              width: `${particle.size}px`,
              height: `${particle.size}px`,
              background: PALETTE[particle.tone].particle,
              '--tx': particle.tx,
              '--ty': particle.ty,
              animationDelay: particle.delay,
            }}
          />
        ))}

        <button
          type="button"
          className="livyo__body"
          onClick={handleManualTip}
          aria-label="Mostra un suggerimento rapido"
        >
          <svg className="livyo__svg" viewBox="0 0 64 64" role="img" aria-hidden="true">
            <defs>
              <linearGradient id="livyoGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stopColor={palette.accent} />
                <stop offset="100%" stopColor={palette.base} />
              </linearGradient>
            </defs>

            <path
              d="M36 11c10.5 0 19 8.5 19 19 0 10-8 18.2-18 19v6.5c0 .8-.9 1.2-1.5.7L30 52.1l-4.5 4.1c-.6.5-1.5.1-1.5-.7V49C14 47.8 6 39.5 6 29c0-10.5 8.5-19 19-19Z"
              fill="url(#livyoGradient)"
            />
            <path
              d="M36 11c10.5 0 19 8.5 19 19 0 10-8 18.2-18 19v6.5c0 .8-.9 1.2-1.5.7L30 52.1l-4.5 4.1c-.6.5-1.5.1-1.5-.7V49C14 47.8 6 39.5 6 29c0-10.5 8.5-19 19-19Z"
              fill="none"
              stroke="rgba(255,255,255,0.45)"
              strokeWidth="1.2"
            />

            <ellipse cx="28" cy="29" rx="3.2" ry="5.4" fill="#fff" />
            <ellipse cx="42" cy="29" rx="3.2" ry="5.4" fill="#fff" />
            <circle cx="28" cy="30" r="1.5" fill="#1E1B4B" />
            <circle cx="42" cy="30" r="1.5" fill="#1E1B4B" />
            <path
              d="M28 39c2.1 1.6 5.9 1.6 8 0"
              stroke="#F8FAFC"
              strokeWidth="2.1"
              strokeLinecap="round"
            />

            {mood === 'skip' && (
              <path
                className="livyo__tear"
                d="M45.5 35.5c1.4 1.8 1.8 3.8.7 5.5"
                stroke="#DBEAFE"
                strokeWidth="1.4"
                strokeLinecap="round"
              />
            )}

            {mood === 'error' && (
              <path
                className="livyo__tear livyo__tear--delayed"
                d="M23.5 35.5c-1.4 1.8-1.8 3.8-.7 5.5"
                stroke="#DBEAFE"
                strokeWidth="1.4"
                strokeLinecap="round"
              />
            )}
          </svg>
        </button>
      </div>
    </div>
  )
}
