// Central place to resolve the backend base URL.
// CRA env vars are build-time, but we can still make runtime-safe adjustments
// (e.g. when accessing the frontend via a LAN IP).

export function getBackendUrl() {
  const raw = process.env.REACT_APP_BACKEND_URL || "http://localhost:8002";
  if (typeof window === "undefined") return raw;

  // If the app is opened via a non-localhost hostname (LAN IP / DNS),
  // and the backend URL points to localhost/127.0.0.1, rewrite the hostname
  // so API calls go to the same machine serving the frontend.
  try {
    const u = new URL(raw);
    const pageHost = window.location.hostname;
    const isBackendLocal = u.hostname === "localhost" || u.hostname === "127.0.0.1";
    const isPageLocal = pageHost === "localhost" || pageHost === "127.0.0.1";

    if (isBackendLocal && !isPageLocal && pageHost) {
      u.hostname = pageHost;
      // Normalize (remove trailing slash)
      return u.toString().replace(/\/$/, "");
    }
  } catch {
    // ignore invalid URL
  }

  return raw;
}


