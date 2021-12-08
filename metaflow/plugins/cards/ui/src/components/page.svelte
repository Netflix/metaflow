<!-- This component will wrap a page.  Later we can decide if we want to render only a single page -->
<script lang="ts">
  import type * as types from "../types";
  import CardComponentRenderer from "./card-component-renderer.svelte";

  export let componentData: types.PageComponent;
  const { title, contents } = componentData;
</script>

<div id={`page-${title || "No Title"}`}>
  {#each contents || [] as component}
    <CardComponentRenderer componentData={component} />
  {/each}
</div>
