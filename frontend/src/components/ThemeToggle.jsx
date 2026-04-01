import { MoonIcon, SunIcon } from './Icons'
import { useTheme } from '../theme/theme-context'

export default function ThemeToggle({ compact = false }) {
  const { theme, setTheme } = useTheme()

  return (
    <div
      className={`theme-toggle${compact ? ' theme-toggle--compact' : ''}`}
      role="group"
      aria-label="Selettore tema"
    >
      <span
        className={`theme-toggle__thumb${theme === 'dark' ? ' is-dark' : ''}`}
        aria-hidden="true"
      />

      <button
        type="button"
        className={`theme-toggle__option${theme === 'light' ? ' is-active' : ''}`}
        onClick={() => setTheme('light')}
        aria-label="Attiva tema chiaro"
      >
        <SunIcon />
        {!compact && <span>Chiaro</span>}
      </button>

      <button
        type="button"
        className={`theme-toggle__option${theme === 'dark' ? ' is-active' : ''}`}
        onClick={() => setTheme('dark')}
        aria-label="Attiva tema scuro"
      >
        <MoonIcon />
        {!compact && <span>Scuro</span>}
      </button>
    </div>
  )
}
