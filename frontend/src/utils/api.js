function trimTrailingSlash(value) {
  return String(value || '').replace(/\/+$/, '')
}

function isLoopbackHost(hostname) {
  return hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '[::1]'
}

function isPrivateIpv4(hostname) {
  return /^(10\.|127\.|192\.168\.|172\.(1[6-9]|2\d|3[0-1])\.)/.test(hostname)
}

function buildOriginLike(url, hostname) {
  return `${url.protocol}//${hostname}${url.port ? `:${url.port}` : ''}`
}

export function resolveApiBaseUrl(configuredValue = import.meta.env.VITE_API_URL) {
  const rawValue = trimTrailingSlash(configuredValue)
  if (!rawValue) {
    return ''
  }

  try {
    const configuredUrl = new URL(rawValue)

    if (typeof window === 'undefined') {
      return trimTrailingSlash(configuredUrl.toString())
    }

    const currentHost = window.location.hostname
    const currentIsLoopback = isLoopbackHost(currentHost)
    const configuredIsLoopback = isLoopbackHost(configuredUrl.hostname)
    const currentIsPrivate = isPrivateIpv4(currentHost)
    const configuredIsPrivate = isPrivateIpv4(configuredUrl.hostname)

    if (currentIsLoopback && configuredIsPrivate && !configuredIsLoopback) {
      return buildOriginLike(configuredUrl, '127.0.0.1')
    }

    if (!currentIsLoopback && currentIsPrivate && configuredIsLoopback) {
      return buildOriginLike(configuredUrl, currentHost)
    }

    return trimTrailingSlash(configuredUrl.toString())
  } catch {
    return rawValue
  }
}

export const API_BASE_URL = resolveApiBaseUrl()

export function getApiDisplayUrl() {
  return API_BASE_URL || trimTrailingSlash(import.meta.env.VITE_API_URL) || 'URL API non configurato'
}

export function getApiErrorMessage(error) {
  if (error?.response?.status === 404) {
    return "Il servizio risponde, ma l'endpoint richiesto non esiste."
  }

  if (error?.response?.status >= 500) {
    return 'Il backend ha risposto con un errore interno. Riprova tra poco.'
  }

  if (error?.code === 'ERR_NETWORK' || /network error/i.test(String(error?.message || ''))) {
    return `Non riesco a raggiungere il backend su ${getApiDisplayUrl()}. Verifica che sia avviato.`
  }

  return 'Non riesco a completare la richiesta in questo momento.'
}
