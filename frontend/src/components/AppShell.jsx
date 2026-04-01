import { NavLink } from 'react-router-dom'
import { CompassIcon, HeartIcon, UserIcon } from './Icons'
import ThemeToggle from './ThemeToggle'

const navItems = [
  { to: '/swipe', label: 'Scopri', icon: CompassIcon },
  { to: '/preferiti', label: 'Salvati', icon: HeartIcon },
  { to: '/profile', label: 'Profilo', icon: UserIcon },
]

export default function AppShell({
  title = 'Casinder',
  leftSlot,
  rightSlot,
  children,
  showNav = true,
  showThemeToggle = true,
  bodyClassName = '',
  shellClassName = '',
}) {
  return (
    <div className={`screen-shell${shellClassName ? ` ${shellClassName}` : ''}`}>
      <div className="phone-frame">
        <header className="topbar">
          <div className="topbar__inner">
            <div className="topbar__side topbar__side--left">
              {leftSlot || <span className="topbar__placeholder" />}
            </div>
            <div className="topbar__title">{title}</div>
            <div className="topbar__side topbar__side--right">
              {rightSlot}
              {showThemeToggle && <ThemeToggle compact />}
            </div>
          </div>
        </header>

        <main className={`screen-body${bodyClassName ? ` ${bodyClassName}` : ''}`}>
          <div className="screen-body__inner">{children}</div>
        </main>

        {showNav && (
          <nav className="bottom-nav" aria-label="Navigazione principale">
            <div className="bottom-nav__inner">
              {navItems.map((item) => {
                const Icon = item.icon

                return (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    end={item.to === '/swipe'}
                    className={({ isActive }) => `bottom-nav__item${isActive ? ' is-active' : ''}`}
                  >
                    <Icon />
                    <span>{item.label}</span>
                  </NavLink>
                )
              })}
            </div>
          </nav>
        )}
      </div>
    </div>
  )
}
