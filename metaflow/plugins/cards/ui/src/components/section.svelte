<!-- this creates a section wrapper to hold multiple components inside it -->
<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.SectionComponent;
  $: ({ title, subtitle, columns } = componentData);
  let style: string;

  if (columns) {
    style = `grid-template-columns: repeat(${columns || 1}, 1fr);`;
  }
</script>

<section
  class="container"
  class:columns
  data-component="section"
  data-section-id={title}
>
  <div class="heading">
    {#if title}
      <h3>{title}</h3>
    {/if}
    {#if subtitle}
      <p class="description">{subtitle}</p>
    {/if}
  </div>
  <div class="sectionItems" {style}>
    <slot />
  </div>
  <hr />
</section>

<style>
  .heading {
    margin-bottom: 1.5rem;
  }

  .sectionItems {
    display: block;
  }

  /* doing this to prevent highly proportioned images from taking up too much height when they're in columns */
  :global(.sectionItems .imageContainer) {
    max-height: 500px;
  }

  .container {
    scroll-margin: var(--component-spacer);
  }

  hr {
    background: var(--grey);
    border: none;
    height: 1px;
    margin: var(--component-spacer) 0;
    padding: 0;
  }

  @media (min-width: 60rem) {
    .sectionItems {
      display: grid;
      grid-gap: 2rem;
    }
  }
</style>
