/**
 * Theme composable — tracks the user's choice (system / light / dark),
 * resolves it against prefers-color-scheme, and applies a data-theme
 * attribute to <html> that the CSS tokens in style.css key off.
 *
 * The store is module-scoped (not per-component) so every call to
 * useTheme() shares state — reading mode from two places always shows
 * the same value. Initialization runs once at module load.
 */
import { computed, ref, watchEffect } from 'vue'

export type ThemeMode = 'system' | 'light' | 'dark'
export type ResolvedTheme = 'light' | 'dark'

const STORAGE_KEY = 'strata.theme'
const VALID_MODES: readonly ThemeMode[] = ['system', 'light', 'dark'] as const

function loadStoredMode(): ThemeMode {
  if (typeof window === 'undefined') return 'system'
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    if (raw && (VALID_MODES as readonly string[]).includes(raw)) {
      return raw as ThemeMode
    }
  } catch {
    // localStorage disabled (private mode, quota, etc.) — fall through.
  }
  return 'system'
}

function systemPrefersDark(): boolean {
  if (typeof window === 'undefined' || !window.matchMedia) return true
  return window.matchMedia('(prefers-color-scheme: dark)').matches
}

const mode = ref<ThemeMode>(loadStoredMode())
// Mirror of the OS preference so `mode === 'system'` can react live.
const systemDark = ref<boolean>(systemPrefersDark())

if (typeof window !== 'undefined' && window.matchMedia) {
  const mql = window.matchMedia('(prefers-color-scheme: dark)')
  mql.addEventListener('change', (e) => {
    systemDark.value = e.matches
  })
}

const resolved = computed<ResolvedTheme>(() => {
  if (mode.value === 'system') return systemDark.value ? 'dark' : 'light'
  return mode.value
})

// Apply data-theme to <html> and persist whenever the user flips the
// switch. watchEffect runs once on registration so first paint matches.
watchEffect(() => {
  if (typeof document !== 'undefined') {
    document.documentElement.dataset.theme = resolved.value
  }
  if (typeof window !== 'undefined') {
    try {
      window.localStorage.setItem(STORAGE_KEY, mode.value)
    } catch {
      // Persisting is best-effort.
    }
  }
})

function setMode(next: ThemeMode) {
  mode.value = next
}

function cycleMode() {
  // system → light → dark → system (matches the order shown in the UI).
  const order: ThemeMode[] = ['system', 'light', 'dark']
  const idx = order.indexOf(mode.value)
  mode.value = order[(idx + 1) % order.length]
}

export function useTheme() {
  return {
    mode,
    resolved,
    setMode,
    cycleMode,
  }
}
