<!-- This component renders the artifacts data viewer -->
<script lang="ts">
  import type * as types from "../types";
  import ArtifactRow from "./artifact-row.svelte";

  export let componentData: types.ArtifactsComponent;

  // we can't guarantee the data is sorted from the source, so we sort before render
  const sortedData = componentData?.data.sort((a, b) => {
    // nulls first
    if (a.name && b.name) {
      if (a.name > b.name) {
        return 1;
      } else if (a.name < b.name) {
        return -1;
      }
    }
    return 0;
  });
</script>

<div class="container" data-component="artifacts">
  <!-- language-python is a prism.js class -->
  <table class="language-python">
    {#each sortedData as artifact}
      <ArtifactRow id={artifact.name} {artifact} />
    {/each}
  </table>
</div>

<style>
  .container {
    width: 100%;
    overflow: auto;
  }

  table {
    width: 100%;
  }
</style>
