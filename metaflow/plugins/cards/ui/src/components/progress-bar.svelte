<!-- The progress bar component -->

<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.ProgressBarComponent;
  // note we removed color for now as an attribute.
  // because it uses the shadow dom, and we want to make it dynamic,
  // we'll have to implement some extra styling options to do this later.
  // lets save this for if its ever requested as a feature.
  $: ({ max, id, value, label, unit, details } = componentData);
  if (value == null) {
    value = 0;
  }
  let displayValue = value.toString();
  $: if (max) {
    displayValue = `${value}/${max}`;
  } else if (unit) {
    displayValue = `${value} ${unit}`;
  }
</script>

<div class="container">
  <div class="inner">
    {#if label || details}
      <div class="info">
        {#if label}
          <label for={id}
            >{label}
            <span class="labelValue"> {displayValue}</span></label
          >
        {/if}
        {#if details}
          <span title={details} class="details">{details}</span>
        {/if}
      </div>
    {/if}
    <progress {id} {max} {value} style={`color: red !important`}
      >{value}{unit || ""}</progress
    >
  </div>
</div>

<style>
  /* styling a progress bar is trickier than it should be. */
  progress::-webkit-progress-bar {
    background-color: white !important;
    min-width: 100%;
  }
  progress {
    background-color: white;
    color: #326cded9 !important;
  }

  progress::-moz-progress-bar {
    background-color: #326cde !important;
  }

  :global(table .container) {
    background: transparent !important;
    font-size: 10px !important;
    padding: 0 !important;
  }

  :global(table progress) {
    height: 4px !important;
  }

  .container {
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 12px;
    border-radius: 3px;
    background: #edf5ff;
    padding: 3rem;
  }

  .inner {
    max-width: 410px;
    width: 100%;
    text-align: center;
  }

  .info {
    display: flex;
    justify-content: space-between;
  }

  :global(table .info) {
    text-align: left;
    flex-direction: column;
  }

  label {
    font-weight: bold;
  }

  .labelValue {
    border-left: 1px solid rgba(0, 0, 0, 0.1);
    margin-left: 0.25rem;
    padding-left: 0.5rem;
  }

  .details {
    font-family: var(--mono-font);
    font-size: 8px;
    color: #333433;
    line-height: 18px;
    overflow: hidden;
    white-space: nowrap;
  }

  progress {
    width: 100%;
    border: none;
    border-radius: 5px;
    height: 8px;
    background: white;
  }
</style>
