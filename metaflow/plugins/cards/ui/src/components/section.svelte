<!-- this creates a section wrapper to hold multiple components inside it -->
<script lang="ts">
  import type * as types from "../types";
  import CardComponentRenderer from "./card-component-renderer.svelte";

  export let style: string | undefined = undefined;
  export let componentData: types.SectionComponent;

  const { title, subtitle, columns, contents } = componentData;
</script>

<section class="container" data-section-id={title}>
  <div class="heading">
    {#if title}
      <h3>{title}</h3>
    {/if}
    {#if subtitle}
      <p class="description">{subtitle}</p>
    {/if}
  </div>
  <div
    class="sectionItems"
    style={`grid-template-columns: repeat(${columns}, 1fr);` + style}
  >
    {#if contents}
      {#each contents || [] as componentData}
        <CardComponentRenderer {componentData} />
      {/each}
    {:else}
      <slot />
    {/if}
  </div>
</section>

<style>
  h3 {
    font-weight: 600;
  }

  .heading {
    margin-bottom: 1.66rem;
  }

  .sectionItems {
    display: block;
  }

  /* doing this to prevent highly proportioned images from taking up too much height when they're in columns */
  :global(.sectionItems .imageContainer) {
    max-height: 50vh;
  }

  .container {
    margin-bottom: calc(var(--component-spacer) / 2);
    padding-bottom: calc(var(--component-spacer) / 2);
    scroll-margin: calc(var(--component-spacer));
  }

  @media (min-width: 60rem) {
    .container {
      margin-bottom: var(--component-spacer);
      padding-bottom: var(--component-spacer);
      scroll-margin: calc(var(--component-spacer) / 2);
    }

    .sectionItems {
      display: grid;
      grid-gap: 2rem;
    }
  }

  @media (min-width: 100rem) {
    .container {
      margin-bottom: calc(var(--component-spacer) * 1.5);
      padding-bottom: calc(var(--component-spacer) * 1.5);
      scroll-margin: calc(var(--component-spacer) / 1.5);
    }

    .sectionItems {
      grid-gap: 4rem;
    }
  }
</style>
