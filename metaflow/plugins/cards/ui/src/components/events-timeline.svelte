<script lang="ts">
  import type * as types from "../types";
  import ComponentRenderer from "./card-component-renderer.svelte";
  import { onMount, onDestroy } from "svelte";

  export let componentData: types.EventsTimelineComponent;

  $: ({ id, title, events, config, stats } = componentData);

  // Get all unique keys from all event metadata to create column headers
  $: allKeys = events.length > 0 ?
    Array.from(new Set(events.flatMap(event => Object.keys(event.metadata)))) : [];


  // Reactive variables for live updates
  let currentTime = Date.now();
  let updateInterval: ReturnType<typeof setInterval>;

  // Update current time every second for relative timestamps (only if not finished)
  onMount(() => {
    if (config.show_relative_time) {
      updateInterval = setInterval(() => {
        currentTime = Date.now();
      }, 1000);
    }
  });

  onDestroy(() => {
    if (updateInterval) {
      clearInterval(updateInterval);
    }
  });

  // Keep relative time ticking even if finished; do not stop interval

  // Format timestamp values for display
  function formatValue(value: any): string {
    if (value === null || value === undefined) {
      return '';
    }

    // Handle timestamp formatting
    if (typeof value === 'number' && value > 1000000000 && value < 10000000000) {
      // Looks like a Unix timestamp
      return new Date(value * 1000).toLocaleString();
    }

    // Handle ISO date strings
    if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)) {
      return new Date(value).toLocaleString();
    }

    // Handle objects and arrays (but not rendered components)
    if (typeof value === 'object' && !isRenderedComponent(value)) {
      return JSON.stringify(value);
    }

    return String(value);
  }

  // Check if a value is a rendered component
  function isRenderedComponent(value: any): boolean {
    return value && typeof value === 'object' && 'type' in value;
  }

  // Get relative time string
  function getRelativeTime(timestamp: number): string {
    const diff = (currentTime - timestamp * 1000) / 1000; // Convert to seconds

    if (diff < 60) {
      return `${Math.floor(diff)}s ago`;
    } else if (diff < 3600) {
      return `${Math.floor(diff / 60)}m ago`;
    } else if (diff < 86400) {
      return `${Math.floor(diff / 3600)}h ago`;
    } else {
      return `${Math.floor(diff / 86400)}d ago`;
    }
  }

  // Get latest known event timestamp (seconds)
  function getLastTimestamp(): number | null {
    if (events && events.length > 0 && typeof events[0].received_at === 'number') {
      return events[0].received_at;
    }
    if (stats && typeof (stats as any).last_update === 'number') {
      return (stats as any).last_update;
    }
    return null;
  }

  function getAgeSeconds(): number | null {
    const ts = getLastTimestamp();
    if (ts == null) return null;
    return Math.max(0, (currentTime - ts * 1000) / 1000);
  }

  function getLastRelativeTime(): string | null {
    const ts = getLastTimestamp();
    if (ts == null) return null;
    return getRelativeTime(ts);
  }

  // Ensure embedded visualizations (e.g., Vega) re-measure after collapse/expand
  function handlePayloadToggle() {
    // Dispatch resize now and shortly after to catch CSS layout
    window.dispatchEvent(new Event('resize'));
    setTimeout(() => window.dispatchEvent(new Event('resize')), 100);
  }

  // local ref for keyed details blocks
  let detailsEl: HTMLDetailsElement | null = null;

  // Get CSS class for different value types
  function getValueClass(key: string, value: any): string {
    if (key.toLowerCase().includes('timestamp') || key.toLowerCase().includes('time')) {
      return 'timestamp';
    }
    if (key.toLowerCase().includes('status')) {
      if (typeof value === 'string') {
        const lowerValue = value.toLowerCase();
        if (lowerValue.includes('success') || lowerValue.includes('completed') || lowerValue.includes('ok')) {
          return 'status-success';
        }
        if (lowerValue.includes('error') || lowerValue.includes('failed') || lowerValue.includes('fail')) {
          return 'status-error';
        }
        if (lowerValue.includes('warning') || lowerValue.includes('pending')) {
          return 'status-warning';
        }
      }
      return 'status';
    }
    if (typeof value === 'number') {
      return 'number';
    }
    return 'default';
  }

  // Get theme class for event styling
  function getEventThemeClass(event: types.EventsTimelineEvent): string {
    if (event.style_theme) {
      return `theme-${event.style_theme}`;
    }
    return 'theme-default';
  }

  // Get priority class for event styling
  function getPriorityClass(event: types.EventsTimelineEvent): string {
    if (event.priority) {
      return `priority-${event.priority}`;
    }
    return 'priority-normal';
  }

  // Format stats for display (for non-time fields)
  function formatStatsValue(key: string, value: any): string {
    if (key === 'events_per_minute' && typeof value === 'number') {
      return `${value}/min`;
    }
    if (key === 'total_runtime_seconds' && typeof value === 'number') {
      return `${value}s`;
    }
    return String(value);
  }

  // Get staleness indicator class (time-based only)
  function getStalenessClass(): string {
    if (stats?.finished) return 'finished';
    const seconds = getAgeSeconds();
    if (seconds == null) return 'live';
    if (seconds < 60) return 'live';
    if (seconds < 300) return 'recent';
    if (seconds < 600) return 'stale';
    return 'finished';
  }

  // Get status text (time-based; finished only affects label)
  function getStatusText(): string {
    const seconds = getAgeSeconds();
    if (stats?.finished) return 'Finished';
    if (seconds == null) return 'Live';
    if (seconds < 60) return 'Live';
    if (seconds < 300) return 'Recent';
    if (seconds < 600) return 'Stale';
    return 'Inactive';
  }
</script>

<div class="events-container" {id}>
  {#if title}
    <h3 class="events-title">{title}</h3>
  {/if}

  <!-- Stats Header -->
  {#if config.show_stats && stats}
    <div class="stats-header {getStalenessClass()}">
      <div class="stats-indicator">
        <span class="live-dot"></span>
        <span class="status-text">
          {getStatusText()}
        </span>
      </div>

      <div class="stats-info">
        <span class="stat-item">
          {stats.displayed_events} of {stats.total_events} events
        </span>
        {#if stats.events_per_minute}
          <span class="stat-item">
            {formatStatsValue('events_per_minute', stats.events_per_minute)}
          </span>
        {/if}
        {#if getLastRelativeTime() !== null}
          <span class="stat-item">
            Last: {getLastRelativeTime()}
          </span>
        {/if}
      </div>
    </div>
  {/if}

  {#if events.length === 0}
    <div class="no-events">
      <p>No events recorded yet.</p>
    </div>
  {:else}
    <div class="events-timeline">
      {#each events as event, index}
        <div class="event-item {getEventThemeClass(event)} {getPriorityClass(event)}"
             class:latest={index === 0}>
          <div class="event-marker"></div>
          <div class="event-content">
            <!-- Event metadata -->
            {#if config.show_relative_time}
              <div class="event-meta">
                <span class="event-time">
                  {getRelativeTime(event.received_at)}
                </span>
                {#if event.event_id}
                  <span class="event-id">#{event.event_id}</span>
                {/if}
              </div>
            {/if}

            <!-- Event metadata -->
            <div class="event-metadata">
              {#each allKeys as key}
                {#if event.metadata[key] !== undefined}
                  <div class="event-field">
                    <span class="field-key">{key}:</span>
                    <div class="field-value {getValueClass(key, event.metadata[key])}">
                      {formatValue(event.metadata[key])}
                    </div>
                  </div>
                {/if}
              {/each}
            </div>

            <!-- Event payloads (collapsible) -->
            {#if Object.keys(event.payloads).length > 0}
              <div class="event-payloads">
                {#each Object.entries(event.payloads) as [payloadKey, payloadComponent]}
                  {#key payloadKey}
                  <details class="payload-section" on:toggle={handlePayloadToggle} bind:this={detailsEl}>
                    <summary class="payload-header">
                      <span class="payload-title">{payloadKey}</span>
                      <span class="payload-type">{payloadComponent.type}</span>
                    </summary>
                    <div class="payload-content">
                      {#key detailsEl?.open}
                        <ComponentRenderer componentData={payloadComponent} />
                      {/key}
                    </div>
                  </details>
                  {/key}
                {/each}
              </div>
            {/if}
          </div>
        </div>
      {/each}
    </div>
  {/if}
</div>

<style>
  .events-container {
    border: 1px solid #e5e7eb;
    border-radius: 0.5rem;
    background: white;
    padding: 1rem;
    margin-bottom: 1rem;
  }

  .events-title {
    font-size: 1.25rem;
    font-weight: 600;
    color: #111827;
    margin: 0 0 1rem 0;
    padding-bottom: 0.5rem;
    border-bottom: 2px solid #e5e7eb;
  }

  /* Stats Header */
  .stats-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.5rem 0.75rem;
    margin-bottom: 1rem;
    border-radius: 0.375rem;
    font-size: 0.875rem;
    border: 1px solid #e5e7eb;
  }

  .stats-header.live {
    background: #f0fdf4;
    border-color: #bbf7d0;
  }

  .stats-header.recent {
    background: #fffbeb;
    border-color: #fed7aa;
  }

  .stats-header.stale {
    background: #fef2f2;
    border-color: #fecaca;
  }

  .stats-indicator {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .live-dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: #10b981;
    box-shadow: 0 0 0 2px rgba(16, 185, 129, 0.2);
    transition: all 0.3s ease;
  }

  .stats-header.live .live-dot {
    background: #10b981;
    box-shadow: 0 0 0 2px rgba(16, 185, 129, 0.3), 0 0 8px rgba(16, 185, 129, 0.4);
    animation: livePulse 2s infinite;
  }

  .stats-header.recent .live-dot {
    background: #f59e0b;
    box-shadow: 0 0 0 2px rgba(245, 158, 11, 0.2);
  }

  .stats-header.stale .live-dot {
    background: #ef4444;
    box-shadow: 0 0 0 2px rgba(239, 68, 68, 0.2);
  }

  .stats-header.finished {
    background: #f0f9ff;
    border-color: #bae6fd;
  }

  .stats-header.finished .live-dot {
    background: #0ea5e9;
    box-shadow: 0 0 0 2px rgba(14, 165, 233, 0.2);
  }

  .status-text {
    font-weight: 500;
    color: #374151;
  }

  .stats-info {
    display: flex;
    gap: 1rem;
    font-size: 0.8125rem;
    color: #6b7280;
  }

  .stat-item {
    white-space: nowrap;
  }

  .no-events {
    text-align: center;
    padding: 2rem;
    color: #6b7280;
    font-style: italic;
  }

  .events-timeline {
    position: relative;
    padding-left: 2rem;
  }

  .events-timeline::before {
    content: '';
    position: absolute;
    left: 0.75rem;
    top: 0;
    bottom: 0;
    width: 2px;
    background: linear-gradient(to bottom, #3b82f6, #e5e7eb);
  }

  .event-item {
    position: relative;
    margin-bottom: 1.5rem;
    padding-left: 1rem;
  }

  .event-item:last-child {
    margin-bottom: 0;
  }

  .event-marker {
    position: absolute;
    left: -1.525rem;
    top: 0;
    width: 0.75rem;
    height: 0.75rem;
    border-radius: 50%;
    background: #3b82f6;
    border: 2px solid white;
    box-shadow: 0 0 0 2px #3b82f6;
    z-index: 1;
  }

  .event-item.latest .event-marker {
    background: #10b981;
    box-shadow: 0 0 0 2px rgba(16, 185, 129, 0.3);
    animation: markerPulse 2s infinite;
  }

  @keyframes livePulse {
    0%, 100% {
      transform: scale(1);
      box-shadow: 0 0 0 2px rgba(16, 185, 129, 0.3), 0 0 8px rgba(16, 185, 129, 0.4);
    }
    50% {
      transform: scale(1.1);
      box-shadow: 0 0 0 4px rgba(16, 185, 129, 0.2), 0 0 12px rgba(16, 185, 129, 0.6);
    }
  }

  @keyframes markerPulse {
    0%, 100% {
      box-shadow: 0 0 0 2px rgba(16, 185, 129, 0.3);
    }
    50% {
      box-shadow: 0 0 0 4px rgba(16, 185, 129, 0.5);
    }
  }

  .event-content {
    background: #f9fafb;
    border: 1px solid #e5e7eb;
    border-radius: 0.375rem;
    padding: 0.75rem;
    box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
  }

  .event-item.latest .event-content {
    background: #f0f9ff;
    border-color: #3b82f6;
  }

  /* Event Theme Styles */
  .event-item.theme-success .event-content {
    background: #f0fdf4;
    border-color: #bbf7d0;
  }

  .event-item.theme-success .event-marker {
    background: #10b981;
    box-shadow: 0 0 0 2px #10b981;
  }

  .event-item.theme-error .event-content {
    background: #fef2f2;
    border-color: #fecaca;
  }

  .event-item.theme-error .event-marker {
    background: #ef4444;
    box-shadow: 0 0 0 2px #ef4444;
  }

  .event-item.theme-warning .event-content {
    background: #fffbeb;
    border-color: #fed7aa;
  }

  .event-item.theme-warning .event-marker {
    background: #f59e0b;
    box-shadow: 0 0 0 2px #f59e0b;
  }

  .event-item.theme-info .event-content {
    background: #eff6ff;
    border-color: #bfdbfe;
  }

  .event-item.theme-info .event-marker {
    background: #3b82f6;
    box-shadow: 0 0 0 2px #3b82f6;
  }

  .event-item.theme-tool_call .event-content {
    background: #f3e8ff;
    border-color: #c4b5fd;
  }

  .event-item.theme-tool_call .event-marker {
    background: #8b5cf6;
    box-shadow: 0 0 0 2px #8b5cf6;
  }

  .event-item.theme-ai_response .event-content {
    background: #fdf4ff;
    border-color: #e9d5ff;
  }

  .event-item.theme-ai_response .event-marker {
    background: #a855f7;
    box-shadow: 0 0 0 2px #a855f7;
  }

  /* Priority Styles */
  .event-item.priority-high {
    border-left: 4px solid #f59e0b;
  }

  .event-item.priority-critical {
    border-left: 4px solid #ef4444;
  }

  /* Event Metadata */
  .event-meta {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.5rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid #e5e7eb;
    font-size: 0.75rem;
    color: #6b7280;
  }

  .event-time {
    font-weight: 500;
  }

  .event-id {
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    opacity: 0.7;
  }

  .event-metadata {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 0.5rem;
  }

  .event-payloads {
    margin-top: 1rem;
    border-top: 1px solid #e5e7eb;
    padding-top: 0.75rem;
  }

  .payload-section {
    margin-bottom: 0.75rem;
    border: 1px solid #e5e7eb;
    border-radius: 0.375rem;
    overflow: hidden;
  }

  .payload-section:last-child {
    margin-bottom: 0;
  }

  .payload-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.75rem 1rem;
    background: #f9fafb;
    cursor: pointer;
    user-select: none;
    border-bottom: 1px solid #e5e7eb;
    font-weight: 500;
  }

  .payload-header:hover {
    background: #f3f4f6;
  }

  .payload-title {
    font-size: 0.875rem;
    color: #374151;
  }

  .payload-type {
    font-size: 0.75rem;
    color: #6b7280;
    background: #e5e7eb;
    padding: 0.125rem 0.5rem;
    border-radius: 0.25rem;
    text-transform: uppercase;
    letter-spacing: 0.025em;
  }

  .payload-content {
    padding: 1rem;
    background: white;
    /* Reset any inherited styles that might interfere */
    font-size: inherit;
    line-height: inherit;
    color: inherit;
  }

  /* Ensure nested components have proper spacing */
  .payload-content :global(> *) {
    margin-bottom: 0;
  }

  .payload-content :global(> *:not(:last-child)) {
    margin-bottom: 1rem;
  }

  /* Allow Prism's default CSS to control PythonCode styling */
  .payload-content :global(pre[data-component="pythonCode"]) {
    margin: 0;
    overflow-x: auto;
  }

  .payload-content :global(pre[data-component="pythonCode"] code) {
    display: block;
  }

  /* Remove token color overrides to defer to Prism theme */

  /* Avoid overriding generic <pre> blocks; let embedded components style themselves */

  .payload-content :global(code:not(pre code)) {
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 0.875rem;
    background: #f1f5f9;
    padding: 0.125rem 0.25rem;
    border-radius: 0.25rem;
  }

  .event-field {
    display: flex;
    flex-direction: column;
    gap: 0.125rem;
  }

  .field-key {
    font-size: 0.75rem;
    font-weight: 500;
    color: #6b7280;
    text-transform: uppercase;
    letter-spacing: 0.025em;
  }

  .field-value {
    font-size: 0.875rem;
    font-weight: 400;
    color: #111827;
    word-break: break-word;
    display: inline-block;
    width: auto;
  }

  .field-value.timestamp {
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 0.8125rem;
    color: #6366f1;
  }

  .field-value.number {
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    color: #059669;
  }

  .field-value.status,
  .field-value.status-success,
  .field-value.status-error,
  .field-value.status-warning,
  .field-value.status-milestone,
  .field-value.status-completed,
  .field-value.status-finished {
    font-weight: 500;
    text-transform: uppercase;
    font-size: 0.75rem;
    letter-spacing: 0.025em;
    padding: 0.125rem 0.375rem;
    border-radius: 0.25rem;
    display: inline-flex !important;
    align-items: center;
    justify-content: center;
    background: #f3f4f6;
    color: #374151;
    width: fit-content !important;
    max-width: fit-content !important;
    min-width: auto !important;
    flex-shrink: 0;
    white-space: nowrap;
  }

  .field-value.status-success {
    color: #059669;
    background: #d1fae5;
  }

  .field-value.status-error {
    color: #dc2626;
    background: #fee2e2;
  }

  .field-value.status-warning {
    color: #d97706;
    background: #fef3c7;
  }

  .field-value.status-milestone {
    color: #7c3aed;
    background: #ede9fe;
  }

  .field-value.status-completed {
    color: #059669;
    background: #d1fae5;
  }

  .field-value.status-finished {
    color: #0ea5e9;
    background: #e0f2fe;
  }

  /* Responsive adjustments */
  @media (max-width: 640px) {
    .events-container {
      padding: 0.75rem;
    }

    .events-timeline {
      padding-left: 1.5rem;
    }

    .stats-info {
      flex-direction: column;
      gap: 0.25rem;
      align-items: flex-end;
    }

    .event-metadata {
      grid-template-columns: 1fr;
      gap: 0.375rem;
    }

    .event-field {
      flex-direction: row;
      align-items: flex-start;
      gap: 0.5rem;
    }

    .field-key {
      min-width: 80px;
      flex-shrink: 0;
    }
  }

  /* Table context adjustments */
  :global(table .events-container) {
    margin: 0.25rem 0;
    border: none;
    background: transparent;
    padding: 0.5rem;
  }

  :global(table .events-timeline) {
    padding-left: 1rem;
  }

  :global(table .event-content) {
    padding: 0.5rem;
  }

  :global(table .stats-header) {
    font-size: 0.75rem;
    padding: 0.375rem 0.5rem;
  }
</style>
