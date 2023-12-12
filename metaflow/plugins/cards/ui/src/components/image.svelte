<!-- This component gives us a wrapper for any image using figures/description/titles -->
<script lang="ts">
  import type * as types from "../types";
  import { modal } from "../store";

  export let componentData: types.ImageComponent;
  $: ({ src, label, description } = componentData);
</script>

<!-- svelte-ignore a11y-click-events-have-key-events a11y-no-noninteractive-element-interactions -->
<figure on:click={() => modal.set(componentData)} data-component="image">
  <div class="imageContainer">
    <img {src} alt={label || "image"} />
  </div>
  {#if label}
    <div class="label">{label}</div>
  {/if}
  {#if description}
    <figcaption class="description">{description}</figcaption>
  {/if}
</figure>

<style>
  figure {
    background: var(--lt-grey);
    padding: 1rem;
    border-radius: 5px;
    text-align: center;
    margin: 0 auto var(--component-spacer);
  }

  @media (min-width: 60rem) {
    figure {
      margin-bottom: 0;
    }
  }

  img {
    max-width: 100%;
    /* arbitrary number here to prevent overflow */
    max-height: 500px;
  }

  .label {
    font-weight: bold;
    margin: 0.5rem 0;
  }

  .description {
    font-size: 0.9rem;
    font-style: italic;
    text-align: center;
    margin: 0.5rem 0;
  }
</style>
