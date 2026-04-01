function IconBase({ children, className = '', ...props }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
      {...props}
    >
      {children}
    </svg>
  )
}

export function SlidersIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M4 6h16" />
      <path d="M4 12h16" />
      <path d="M4 18h16" />
      <circle cx="9" cy="6" r="2" fill="currentColor" stroke="none" />
      <circle cx="15" cy="12" r="2" fill="currentColor" stroke="none" />
      <circle cx="11" cy="18" r="2" fill="currentColor" stroke="none" />
    </IconBase>
  )
}

export function SearchIcon(props) {
  return (
    <IconBase {...props}>
      <circle cx="11" cy="11" r="5.75" />
      <path d="m16 16 4.25 4.25" />
    </IconBase>
  )
}

export function CompassIcon(props) {
  return (
    <IconBase {...props}>
      <circle cx="12" cy="12" r="8.25" />
      <path d="m9 15 1.95-5.05L16 8l-1.95 5.05Z" />
      <path d="m10.95 9.95 2.1 2.1" />
    </IconBase>
  )
}

export function HomeIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M4 11.5 12 5l8 6.5" />
      <path d="M6.5 10.5V19h11v-8.5" />
    </IconBase>
  )
}

export function HeartIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M12 20.25 5.7 13.88a4.18 4.18 0 0 1 5.91-5.91L12 8.36l.39-.39a4.18 4.18 0 0 1 5.91 5.91Z" />
    </IconBase>
  )
}

export function HeartFilledIcon({ className = '', ...props }) {
  return (
    <svg viewBox="0 0 24 24" className={className} aria-hidden="true" {...props}>
      <path
        d="M12 20.25 5.7 13.88a4.18 4.18 0 0 1 5.91-5.91L12 8.36l.39-.39a4.18 4.18 0 0 1 5.91 5.91Z"
        fill="currentColor"
      />
    </svg>
  )
}

export function UserIcon(props) {
  return (
    <IconBase {...props}>
      <circle cx="12" cy="8" r="3.2" />
      <path d="M5.5 19c1.2-3 3.54-4.5 6.5-4.5S17.3 16 18.5 19" />
    </IconBase>
  )
}

export function CloseIcon(props) {
  return (
    <IconBase {...props}>
      <path d="m7 7 10 10" />
      <path d="m17 7-10 10" />
    </IconBase>
  )
}

export function StarIcon(props) {
  return (
    <IconBase {...props}>
      <path d="m12 3.75 2.45 4.96 5.47.8-3.96 3.86.94 5.45L12 16.24l-4.9 2.58.94-5.45L4.08 9.5l5.47-.8Z" />
    </IconBase>
  )
}

export function MapPinIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M12 20s5.25-5.4 5.25-9.5a5.25 5.25 0 1 0-10.5 0C6.75 14.6 12 20 12 20Z" />
      <circle cx="12" cy="10.5" r="1.75" />
    </IconBase>
  )
}

export function LayoutIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M3.75 7.5h16.5" />
      <path d="M5 7.5V16a2.25 2.25 0 0 0 2.25 2.25h9.5A2.25 2.25 0 0 0 19 16V7.5" />
      <path d="M8 13.25h2.5" />
      <path d="M13.5 13.25H16" />
    </IconBase>
  )
}

export function RulerIcon(props) {
  return (
    <IconBase {...props}>
      <path d="m4.75 15.5 10.75-10.75 3.75 3.75L8.5 19.25Z" />
      <path d="m10.2 10.05 1.5 1.5" />
      <path d="m13.2 7.05 1.5 1.5" />
      <path d="m7.2 13.05 1.5 1.5" />
    </IconBase>
  )
}

export function ExternalLinkIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M10 14 20 4" />
      <path d="M14 4h6v6" />
      <path d="M20 13v4a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h4" />
    </IconBase>
  )
}

export function ChevronLeftIcon(props) {
  return (
    <IconBase {...props}>
      <path d="m14.5 6-5 6 5 6" />
    </IconBase>
  )
}

export function ChevronRightIcon(props) {
  return (
    <IconBase {...props}>
      <path d="m9.5 6 5 6-5 6" />
    </IconBase>
  )
}

export function SunIcon(props) {
  return (
    <IconBase {...props}>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2.5v2.25" />
      <path d="M12 19.25v2.25" />
      <path d="m4.93 4.93 1.59 1.59" />
      <path d="m17.48 17.48 1.59 1.59" />
      <path d="M2.5 12h2.25" />
      <path d="M19.25 12h2.25" />
      <path d="m4.93 19.07 1.59-1.59" />
      <path d="m17.48 6.52 1.59-1.59" />
    </IconBase>
  )
}

export function MoonIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M18.5 14.2A6.8 6.8 0 1 1 9.8 5.5a5.8 5.8 0 0 0 8.7 8.7Z" />
    </IconBase>
  )
}

export function RefreshIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M20 11a8 8 0 1 0 2.1 5.4" />
      <path d="M20 4v6h-6" />
    </IconBase>
  )
}

export function TrainIcon(props) {
  return (
    <IconBase {...props}>
      <rect x="6.25" y="4.75" width="11.5" height="11" rx="2.5" />
      <path d="M9 18.75 7.5 20.5" />
      <path d="m15 18.75 1.5 1.75" />
      <path d="M8.5 15.75h7" />
      <path d="M9 8.5h.01" />
      <path d="M15 8.5h.01" />
      <path d="M8.25 11.5h7.5" />
    </IconBase>
  )
}

export function TreeIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M12 4.5 6.5 11h3l-3 4.5h4V19.5" />
      <path d="m12 4.5 5.5 6.5h-3l3 4.5h-4V19.5" />
      <path d="M9.5 19.5h5" />
    </IconBase>
  )
}

export function PhotoStackIcon(props) {
  return (
    <IconBase {...props}>
      <rect x="8" y="7.5" width="10" height="8.5" rx="2" />
      <path d="M13 11.25 15 13.5l1.5-1.75L18 13.5" />
      <circle cx="11" cy="10.5" r="0.8" fill="currentColor" stroke="none" />
      <path d="M6 15.25h-.5A1.75 1.75 0 0 1 3.75 13.5v-5A1.75 1.75 0 0 1 5.5 6.75H14" />
    </IconBase>
  )
}

export function PdfIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M9 4.75h5.75L18 8v11.25H9A2.25 2.25 0 0 1 6.75 17V7A2.25 2.25 0 0 1 9 4.75Z" />
      <path d="M14.75 4.75V8H18" />
      <path d="M9.75 14.5h1.2a1.15 1.15 0 1 0 0-2.3h-1.2v4.05" />
      <path d="M13.2 16.25h1.2a1.65 1.65 0 1 0 0-3.3h-1.2v3.3Z" />
      <path d="M16.7 16.25V13h1.8" />
      <path d="M16.7 14.7h1.5" />
    </IconBase>
  )
}

export function LogOutIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M10 17v2a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v2" />
      <path d="m14 16 5-4-5-4" />
      <path d="M19 12H9" />
    </IconBase>
  )
}

export function ShieldIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M12 3.75 5.5 6.5v4.35c0 4.05 2.3 7.7 6.5 9.4 4.2-1.7 6.5-5.35 6.5-9.4V6.5Z" />
      <path d="m9.7 11.9 1.55 1.55 3.1-3.35" />
    </IconBase>
  )
}
