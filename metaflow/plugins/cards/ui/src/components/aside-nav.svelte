<!-- This component renders the side tree for top-level pages/sections -->
<script lang="ts">
  import type * as types from "../types";
  export let pageHierarchy: types.PageHierarchy = {};

  function handleScroll(section: string): void {
    const el = document.querySelector(`[data-section-id="${section}"]`);
    el?.scrollIntoView({ behavior: "smooth" });
  }
</script>

<nav class="nav">
  <ul class="navList">
    {#each Object.entries(pageHierarchy) as [pageId, sections]}
      <li>
        {#if pageId}
          <span class="pageId">{pageId}</span>
        {/if}
        <ul class="navItem">
          {#each sections || [] as section}
            <li class="sectionLink">
              <button class="textButton" on:click={() => handleScroll(section)}>
                {section}
              </button>
            </li>
          {/each}
        </ul>
      </li>
    {/each}
  </ul>
</nav>

<style>
  .nav {
    border-radius: 0 0 5px 0;
    display: none;
    margin: 0;
    top: 0;
  }

  ul.navList {
    list-style-type: none;
  }

  ul.navList ul {
    margin: 0.5rem 1rem 2rem;
  }

  .navList li {
    display: block;
    margin: 0;
  }

  .navItem li:hover {
    color: var(--blue);
  }

  .pageId {
    display: block;
    border-bottom: 1px solid var(--grey);
    padding: 0 0.5rem;
    margin-bottom: 1rem;
  }

  @media (min-width: 60rem) {
    .nav {
      display: block;
    }

    ul.navList {
      text-align: left;
    }

    .navList li {
      display: block;
      margin: 0.5rem 0;
    }
  }
</style>
