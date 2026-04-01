import { NavLink, useLocation } from 'react-router-dom'
import { HeartIcon, HomeIcon, UserIcon } from './Icons'
import ThemeToggle from './ThemeToggle'

const navItems = [
  { to: '/swipe', label: 'Scopri', icon: HomeIcon },
  { to: '/preferiti', label: 'Salvati', icon: HeartIcon },
  { to: '/profile', label: 'Profilo', icon: UserIcon },
]

export default function AppShell({
  title = 'Casinder',
  leftSlot,
  rightSlot,
  children,
  showNav = true,
  bodyClassName = '',
}) {
  const location = useLocation()
  const activeIndex = Math.max(
    0,
    navItems.findIndex((item) => location.pathname === item.to || location.pathname.startsWith(`${item.to}/`)),
  )

  return (
    <div className="screen-shell">
      <div className="phone-frame">
        <header className="topbar">
          <div className="topbar__inner">
            <div className="topbar__side topbar__side--left">
              {leftSlot || <span className="topbar__placeholder" />}
            </div>
            <div className="topbar__title">{title}</div>
            <div className="topbar__side topbar__side--right">
              {rightSlot}
              <ThemeToggle compact />
            </div>
          </div>
        </header>

        <main className={`screen-body${bodyClassName ? ` ${bodyClassName}` : ''}`}>
          <div className="screen-body__inner">{children}</div>
        </main>

        {showNav && (
          <nav className="bottom-nav" aria-label="Navigazione principale">
            <div className="bottom-nav__inner" style={{ '--nav-index': activeIndex }}>
              <span className="bottom-nav__thumb" aria-hidden="true" />
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
