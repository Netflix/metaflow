<!-- This component gives us a wrapper for main aside components -->
<script lang="ts">
  import Logo from "./logo.svelte";
</script>

<aside>
  <div>
    <div class="logoContainer">
      <Logo />
    </div>
    <slot />
  </div>
</aside>

<style>
  aside {
    display: none;
    line-height: 2;
    text-align: left;
    /* background: var(--lt-lt-grey); */
  }

  @media (min-width: 60rem) {
    aside {
      display: flex;
      flex-direction: column;
      height: 100vh;
      justify-content: space-between;
      padding: 2.5rem 0 1.5rem 1.5rem;
      position: fixed;
      width: var(--aside-width);
    }
  }

  :global(.embed aside) {
    display: none;
  }

  :global(aside ul) {
    list-style-type: none;
  }

  :global(aside a, aside button, aside a:visited) {
    text-decoration: none;
    cursor: pointer;
    font-weight: 700;
    color: var(--black);
  }

  :global(aside a:hover, aside button:hover) {
    text-decoration: underline;
  }

  :global(.logoContainer svg) {
    width: 100%;
    max-width: 140px;
    margin-bottom: 3.75rem;
    height: auto;
  }
</style>
