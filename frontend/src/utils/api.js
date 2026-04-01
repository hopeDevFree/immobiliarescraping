const USER_STORAGE_KEY = 'utente'

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

function parseStoredJson(value) {
  try {
    return JSON.parse(value || 'null')
  } catch {
    return null
  }
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

export function getStoredUser() {
  if (typeof window === 'undefined') {
    return null
  }

  const parsed = parseStoredJson(window.localStorage.getItem(USER_STORAGE_KEY))
  return parsed && typeof parsed === 'object' ? parsed : null
}

export function getStoredAuthToken(user = getStoredUser()) {
  return typeof user?.auth_token === 'string' ? user.auth_token.trim() : ''
}

export function getLegacyUserId(user = getStoredUser()) {
  const legacyUserId = Number(user?.id)
  return Number.isInteger(legacyUserId) && legacyUserId > 0 ? legacyUserId : null
}

export function hasAuthenticatedSession(user = getStoredUser()) {
  return Boolean(getStoredAuthToken(user))
}

export function storeUserSession(user) {
  if (typeof window === 'undefined') {
    return
  }

  window.localStorage.setItem(USER_STORAGE_KEY, JSON.stringify(user))
}

export function clearUserSession() {
  if (typeof window === 'undefined') {
    return
  }

  window.localStorage.removeItem(USER_STORAGE_KEY)
}

export function isUnauthorizedError(error) {
  return error?.response?.status === 401
}

export function getAuthHeaders(user = getStoredUser()) {
  const authToken = getStoredAuthToken(user)
  return authToken ? { Authorization: `Bearer ${authToken}` } : {}
}

export function withAuth(config = {}, user = getStoredUser()) {
  const headers = {
    ...getAuthHeaders(user),
    ...(config.headers || {}),
  }

  if (Object.keys(headers).length === 0) {
    return config
  }

  return {
    ...config,
    headers,
  }
}

export function getApiErrorMessage(error) {
  const statusCode = error?.response?.status
  const detail = error?.response?.data?.detail

  if (typeof detail === 'string' && detail.trim()) {
    return detail
  }

  if (statusCode === 401) {
    return 'La sessione non e piu valida. Rientra per continuare.'
  }

  if (statusCode === 404) {
    return "Il servizio risponde, ma l'endpoint richiesto non esiste."
  }

  if (statusCode >= 500) {
    return 'Il backend ha risposto con un errore interno. Riprova tra poco.'
  }

  if (error?.code === 'ERR_NETWORK' || /network error/i.test(String(error?.message || ''))) {
    return `Non riesco a raggiungere il backend su ${getApiDisplayUrl()}. Verifica che sia avviato.`
  }

  return 'Non riesco a completare la richiesta in questo momento.'
}
