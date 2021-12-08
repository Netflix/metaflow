<!-- This component gives us a wrapper for main aside components -->
<aside>
  <slot />
</aside>

<style>
  aside {
    background: var(--black);
    color: var(--lt-grey);
    display: none;
    line-height: 2;
    text-align: left;
  }

  @media (min-width: 60rem) {
    aside {
      display: flex;
      flex-direction: column;
      height: 100vh;
      justify-content: space-between;
      max-width: 400px;
      min-width: 200px;
      padding: 1.5rem 4rem 1.5rem 2rem;
      width: 20%;
    }
  }

  :global(aside ul) {
    list-style-type: none;
  }

  :global(aside a, aside button, aside a:visited) {
    color: var(--lt-grey);
    text-decoration: none;
    cursor: pointer;
  }

  :global(aside a:hover, aside button:hover) {
    color: var(--lt-blue);
    text-decoration: underline;
  }
</style>
