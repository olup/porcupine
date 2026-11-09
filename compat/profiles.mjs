// Local-ID/session transport diagnostics only; person processing is disabled.
export const browserProfile = Object.freeze({
  // Headless Chromium is a bot user agent; still exercise its real network transport.
  opt_out_useragent_filter: true,
  autocapture: false,
  capture_pageview: false,
  capture_pageleave: false,
  capture_dead_clicks: false,
  capture_performance: false,
  capture_heatmaps: false,
  disable_session_recording: true,
  disable_surveys: true,
  disable_external_dependency_loading: true,
  advanced_disable_flags: true,
  disable_compression: true,
  person_profiles: 'never',
  persistence: 'localStorage',
  request_batching: false,
  api_transport: 'fetch',
});
// Product profile: the SDK sends its shared sentinel without browser persistence.
const { persistence: _identifiedPersistence, ...analyticsOptions } = browserProfile;
export const cookielessProfile = Object.freeze({
  ...analyticsOptions,
  cookieless_mode: 'always',
});
// Opt-in identified browser mode. Person processing is separate from local-ID diagnostics.
export const identifiedBrowserProfile = Object.freeze({
  ...browserProfile,
  persistence: 'cookie',
  person_profiles: 'identified_only',
});
export const nodeProfile = Object.freeze({
  flushAt: 100,
  flushInterval: 0,
  enableLocalEvaluation: false,
  disableGeoip: true,
  fetchRetryCount: 2,
  fetchRetryDelay: 20,
  requestTimeout: 3000,
});
