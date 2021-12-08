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
    overflow-y: scroll;
    padding: 3rem;
    width: 100%;
  }

  @media (min-width: 60rem) {
    main {
      padding: 2rem 4.5rem;
    }
  }

  @media (min-width: 80rem) {
    main {
      padding: 2rem 6rem;
    }
  }

  @media (min-width: 100rem) {
    main {
      padding: 2rem 9rem;
    }
  }
</style>
