const priceFormatter = new Intl.NumberFormat('it-IT')
const distanceFormatter = new Intl.NumberFormat('it-IT', {
  minimumFractionDigits: 0,
  maximumFractionDigits: 1,
})

const TEXT_REPLACEMENTS = [
  ['Ã¢â€šÂ¬', '\u20AC'],
  ['Ã‚Â²', '\u00B2'],
  ['Â²', '\u00B2'],
  ['â€™', "'"],
  ['â€œ', '"'],
  ['â€\u009d', '"'],
  ['Ã ', '\u00E0'],
  ['Ã¨', '\u00E8'],
  ['Ã©', '\u00E9'],
  ['Ã¬', '\u00EC'],
  ['Ã²', '\u00F2'],
  ['Ã¹', '\u00F9'],
]

function decodeValue(value) {
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

function repairText(value) {
  let normalized = String(value || '')

  for (const [from, to] of TEXT_REPLACEMENTS) {
    normalized = normalized.replaceAll(from, to)
  }

  return normalized
}

function normalizeTitle(title) {
  const cleaned = repairText(decodeValue(String(title || '')))
    .replace(/[-_]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()

  if (!cleaned) {
    return ''
  }

  return cleaned.charAt(0).toUpperCase() + cleaned.slice(1)
}

function inferLayoutLabel(annuncio) {
  const title = normalizeTitle(annuncio?.titolo).toLowerCase()
  const mapping = [
    { regex: /monolocale/, label: '1 stanza' },
    { regex: /bilocale/, label: '2 stanze' },
    { regex: /trilocale/, label: '3 stanze' },
    { regex: /quadrilocale/, label: '4 stanze' },
    { regex: /(\d)\s+locali?/, label: (_match, rooms) => `${rooms} stanze` },
  ]

  for (const item of mapping) {
    const match = title.match(item.regex)

    if (match) {
      return typeof item.label === 'function' ? item.label(...match) : item.label
    }
  }

  return annuncio?.tipo === 'stanza' ? 'Stanza privata' : 'Appartamento'
}

function extractSurfaceLabel(annuncio) {
  const title = normalizeTitle(annuncio?.titolo)
  const match = title.match(/(\d{2,3})\s?(?:m2|mq|m\u00B2)/i)

  if (!match) {
    return ''
  }

  return `${match[1]} m\u00B2`
}

function getPrimaryArea(annuncio) {
  return annuncio?.microzona?.trim() || annuncio?.macrozona?.trim() || annuncio?.zona?.trim() || ''
}

export function getListingSourceLabel(url) {
  try {
    const hostname = new URL(url).hostname.replace(/^www\./, '')
    const [name] = hostname.split('.')
    return name ? name.charAt(0).toUpperCase() + name.slice(1) : 'Portale'
  } catch {
    return 'Portale'
  }
}

export function formatMonthlyPrice(value) {
  const amount = Number(value)

  if (!Number.isFinite(amount)) {
    return 'Prezzo su richiesta'
  }

  return `\u20AC ${priceFormatter.format(amount)}`
}

export function formatDistanceKm(value) {
  const distance = Number(value)

  if (!Number.isFinite(distance) || distance < 0) {
    return ''
  }

  if (distance < 1) {
    return 'Vicino'
  }

  return `${distanceFormatter.format(distance)} km`
}

export function getListingImages(annuncio) {
  const images = []
  const seen = new Set()

  for (const url of Array.isArray(annuncio?.url_immagini) ? annuncio.url_immagini : []) {
    if (typeof url === 'string' && url && !seen.has(url)) {
      seen.add(url)
      images.push(url)
    }
  }

  if (typeof annuncio?.url_immagine === 'string' && annuncio.url_immagine && !seen.has(annuncio.url_immagine)) {
    images.unshift(annuncio.url_immagine)
  }

  return images
}

export function getListingDescription(annuncio) {
  return repairText(String(annuncio?.descrizione || ''))
    .replace(/\s+/g, ' ')
    .trim()
}

export function getListingTitle(annuncio) {
  const title = normalizeTitle(annuncio?.titolo)
  const zone = getPrimaryArea(annuncio)

  if (title && zone && !title.toLowerCase().includes(zone.toLowerCase())) {
    return `${title} in zona ${zone}`
  }

  if (title) {
    return title
  }

  const fallback = annuncio?.tipo === 'stanza' ? 'Stanza privata' : 'Appartamento'
  return zone ? `${fallback} in zona ${zone}` : fallback
}

export function getListingLayoutLabel(annuncio) {
  return inferLayoutLabel(annuncio)
}

export function getListingSurfaceLabel(annuncio) {
  return extractSurfaceLabel(annuncio)
}

export function getListingPrimaryArea(annuncio) {
  return getPrimaryArea(annuncio)
}

export function getListingLocationLine(annuncio) {
  const city = annuncio?.zona?.trim() || 'Napoli'
  const area = getPrimaryArea(annuncio)

  if (!area || area.toLowerCase() === city.toLowerCase()) {
    return city
  }

  return `${city}, ${area}`
}

export function getListingMeta(annuncio) {
  const meta = [{ key: 'layout', label: inferLayoutLabel(annuncio) }]
  const zone = getPrimaryArea(annuncio)
  const surface = extractSurfaceLabel(annuncio)

  if (zone) {
    meta.push({ key: 'location', label: zone })
  }

  if (surface) {
    meta.push({ key: 'surface', label: surface })
  }

  if (meta.length < 3) {
    meta.push({ key: 'contract', label: 'Affitto' })
  }

  if (meta.length < 3) {
    meta.push({ key: 'source', label: getListingSourceLabel(annuncio?.url) })
  }

  return meta.slice(0, 3)
}
