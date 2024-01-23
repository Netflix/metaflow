<!-- A line that connects each step to the next. -->
<script lang="ts">
  // In rems
  export let top = 0;
  export let left = 0;
  export let bottom = 0;
  export let right = 0;

  // Note: also defined in CSS
  const strokeWidth = 0.5;

  let shouldFlip: boolean;
  let width: number;
  let height: number;
  let straightLine = false;

  $: {
    shouldFlip = right - left < 0;
    width = Math.abs(right - left);

    // Safari does not render a box with zero width
    if (width <= strokeWidth) {
      width = strokeWidth;
      straightLine = true;
      left -= strokeWidth / 2;
    } else {
      // Move sides so the border is centered
      if (shouldFlip) {
        left += strokeWidth / 2;
        right -= strokeWidth / 2;
      } else {
        left -= strokeWidth / 2;
        right += strokeWidth / 2;
      }
      width = Math.abs(right - left);
    }
    height = bottom - top;
  }
</script>

<div
  class="connectorwrapper"
  class:flip={shouldFlip}
  style="top: {top}rem; left: {left}rem; width: {width}rem; height: {height}rem;"
>
  {#if straightLine}
    <div class="path straightLine" />
  {:else}
    <div class="path topLeft" />
    <div class="path bottomRight" />
  {/if}
</div>

<style>
  .connectorwrapper {
    transform-origin: 0 0;
    position: absolute;
    z-index: 0;
    min-width: var(--strokeWidth);
  }

  .flip {
    transform: scaleX(-1);
  }

  .path {
    /* Note: also defined in JS */
    --strokeWidth: 0.5rem;
    --strokeColor: var(--dag-connector);
    --borderRadius: 1.25rem;
    box-sizing: border-box;
  }

  .straightLine {
    position: absolute;
    top: 0;
    bottom: 0;
    left: 0;
    right: 0;
    border-left: var(--strokeWidth) solid var(--strokeColor);
  }

  .topLeft {
    position: absolute;
    top: 0;
    left: 0;
    right: 50%;
    bottom: calc(var(--dag-gap) / 2 - var(--strokeWidth) / 2);
    border-radius: 0 0 0 var(--borderRadius);
    border-left: var(--strokeWidth) solid var(--strokeColor);
    border-bottom: var(--strokeWidth) solid var(--strokeColor);
  }

  .bottomRight {
    position: absolute;
    top: calc(100% - (var(--dag-gap) / 2 + var(--strokeWidth) / 2));
    left: 50%;
    right: 0;
    bottom: 0;
    border-radius: 0 var(--borderRadius) 0 0;
    border-top: var(--strokeWidth) solid var(--strokeColor);
    border-right: var(--strokeWidth) solid var(--strokeColor);
  }
</style>
