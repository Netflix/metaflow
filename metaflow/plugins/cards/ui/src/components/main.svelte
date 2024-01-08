<!-- This component gives us a wrapper for the main section -->
<main>
  <div class="mainContainer">
    <slot />
  </div>
</main>

<style>
  .mainContainer {
    max-width: 110rem;
  }

  main {
    flex: 0 1 auto;
    max-width: 100rem;
    padding: 1.5rem;
  }

  @media (min-width: 60rem) {
    main {
      margin-left: var(--aside-width);
    }
  }

  /* if the embed class is present, we hide the aside, and we should center the main */
  :global(.embed main) {
    margin: 0 auto;
    min-width: 80%
  }
</style>
