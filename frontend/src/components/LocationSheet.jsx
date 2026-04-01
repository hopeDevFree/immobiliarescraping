import { CloseIcon, MapPinIcon } from './Icons'

export default function LocationSheet({
  open,
  positions,
  loading,
  error,
  draftLocation,
  onPickLocation,
  onRadiusChange,
  onApply,
  onReset,
  onClose,
}) {
  if (!open) {
    return null
  }

  const radiusIsActive = draftLocation.locationKind === 'zone'

  return (
    <div className="filter-sheet-layer">
      <button
        type="button"
        className="filter-sheet__backdrop"
        aria-label="Chiudi selezione posizione"
        onClick={onClose}
      />

      <section className="filter-sheet location-sheet" role="dialog" aria-modal="true" aria-labelledby="location-sheet-title">
        <div className="filter-sheet__top">
          <div className="filter-sheet__grabber" aria-hidden="true" />

          <div className="filter-sheet__header">
            <div className="filter-sheet__heading">
              <h2 id="location-sheet-title" className="filter-sheet__title">Posizione e raggio</h2>
            </div>

            <button
              type="button"
              className="topbar__action filter-sheet__close"
              onClick={onClose}
              aria-label="Chiudi selezione posizione"
            >
              <CloseIcon />
            </button>
          </div>
        </div>

        <div className="filter-sheet__content">
          <section className="filter-section">
            <p className="filter-section__label">Posizione di riferimento</p>

            {error && <p className="inline-message inline-message--error">{error}</p>}

            <div className="location-option-list">
              {loading && (
                <p className="inline-message">Sto cercando le zone migliori da proporti.</p>
              )}

              {positions.map((position) => {
                const isActive = draftLocation.locationId === position.id
                const listingCountLabel = position.listing_count === 1
                  ? '1 annuncio vicino a questa zona'
                  : `${position.listing_count} annunci vicini a questa zona`

                return (
                  <button
                    key={position.id}
                    type="button"
                    className={`location-option${isActive ? ' is-active' : ''}`}
                    onClick={() => onPickLocation(position)}
                  >
                    <span className="location-option__main">
                      <span className="location-option__icon">
                        <MapPinIcon />
                      </span>
                      <span className="location-option__text">
                        <strong>{position.label}</strong>
                        <small>
                          {position.kind === 'all'
                            ? 'Tutti gli annunci disponibili'
                            : listingCountLabel}
                        </small>
                      </span>
                    </span>
                    {isActive && <span className="location-option__check">Attiva</span>}
                  </button>
                )
              })}

              {!loading && positions.length === 0 && (
                <div className="state-card">
                  <h2>Nessuna posizione disponibile</h2>
                  <p>Appena arrivano annunci con coordinate, qui troverai le zone selezionabili.</p>
                </div>
              )}
            </div>
          </section>

          <section className="filter-section">
            <div className="location-radius__header">
              <p className="filter-section__label">Raggio</p>
              <span className="location-radius__value">{draftLocation.radiusKm} km</span>
            </div>
            <input
              className="location-radius__input"
              type="range"
              min="1"
              max="20"
              step="1"
              value={draftLocation.radiusKm}
              onChange={(event) => onRadiusChange(Number(event.target.value))}
              disabled={!radiusIsActive}
            />
            <p className="location-radius__hint">
              {radiusIsActive
                ? 'Mostra gli annunci entro il raggio scelto dalla zona selezionata.'
                : 'Seleziona una zona per attivare davvero il filtro in km.'}
            </p>
          </section>
        </div>

        <div className="filter-sheet__footer">
          <button type="button" className="btn btn--ghost" onClick={onReset}>
            Reset
          </button>
          <button type="button" className="btn btn--primary" onClick={onApply}>
            Applica
          </button>
        </div>
      </section>
    </div>
  )
}
