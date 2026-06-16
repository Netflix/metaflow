<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.ValueBoxComponent;

  $: ({ id, title, value, subtitle, theme, change_indicator } = componentData);

  // Format the value for display
  $: displayValue = typeof value === 'number' ? value.toLocaleString() : value;
</script>

<div class="value-box {theme || 'default'}" {id}>
  <div class="value-box-content">
    {#if title}
      <h3 class="value-box-title">{title}</h3>
    {/if}

    <div class="value-box-value">{displayValue}</div>

    {#if subtitle}
      <p class="value-box-subtitle">{subtitle}</p>
    {/if}

    {#if change_indicator}
      <div class="value-box-change">{change_indicator}</div>
    {/if}
  </div>
</div>

<style>
  .value-box {
    border-radius: 0.5rem;
    padding: 1.5rem;
    box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);
    border: 1px solid #e5e7eb;
    background: white;
    min-height: 120px;
    display: flex;
    align-items: center;
  }

  .value-box-content {
    width: 100%;
  }

  .value-box-title {
    font-size: 0.875rem;
    font-weight: 500;
    color: #6b7280;
    margin: 0 0 0.5rem 0;
    text-transform: uppercase;
    letter-spacing: 0.025em;
  }

  .value-box-value {
    font-size: 2rem;
    font-weight: 700;
    color: #111827;
    line-height: 1.2;
    margin: 0 0 0.5rem 0;
  }

  .value-box-subtitle {
    font-size: 0.875rem;
    color: #6b7280;
    margin: 0 0 0.5rem 0;
  }

  .value-box-change {
    font-size: 0.75rem;
    font-weight: 500;
    color: #059669;
    text-transform: uppercase;
    letter-spacing: 0.025em;
  }

  /* Theme variants */
  .value-box.default {
    background: white;
    border-color: #e5e7eb;
  }

  .value-box.bg-gradient-indigo-purple {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    border: none;
  }

  .value-box.bg-gradient-indigo-purple .value-box-title,
  .value-box.bg-gradient-indigo-purple .value-box-subtitle {
    color: rgba(255, 255, 255, 0.8);
  }

  .value-box.bg-gradient-indigo-purple .value-box-value {
    color: white;
  }

  .value-box.bg-gradient-indigo-purple .value-box-change {
    color: rgba(255, 255, 255, 0.9);
  }


  .value-box.success {
    background: #f0fdf4;
    border-color: #bbf7d0;
  }

  .value-box.success .value-box-value {
    color: #065f46;
  }

  .value-box.success .value-box-change {
    color: #059669;
  }

  .value-box.warning {
    background: #fffbeb;
    border-color: #fed7aa;
  }

  .value-box.warning .value-box-value {
    color: #92400e;
  }

  .value-box.warning .value-box-change {
    color: #d97706;
  }

  .value-box.danger {
    background: #fef2f2;
    border-color: #fecaca;
  }

  .value-box.danger .value-box-value {
    color: #991b1b;
  }

  .value-box.danger .value-box-change {
    color: #dc2626;
  }

  /* Responsive adjustments */
  @media (max-width: 640px) {
    .value-box {
      padding: 1rem;
      min-height: 100px;
    }

    .value-box-value {
      font-size: 1.5rem;
    }

  }

  /* Table context adjustments */
  :global(table .value-box) {
    min-height: auto;
    padding: 0.75rem;
    margin: 0.25rem 0;
  }

  :global(table .value-box-value) {
    font-size: 1.25rem;
  }

</style>
