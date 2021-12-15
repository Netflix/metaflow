<!-- This component renders the artifacts data viewer -->
<script lang="ts">
  import type * as types from "../types";
  import ArtifactRow from "./artifact-row.svelte";

  export let componentData: types.ArtifactsComponent;
  const { data } = componentData;

  // we can't guarantee the data is sorted from the source, so we sort before render
  const sortedData = Object.entries(data).sort((a, b) => {
    if (a[0] > b[0]) {
      return 1;
    } else if (a[0] < b[0]) {
      return -1;
    }
    return 0;
  });
</script>

<div class="tableContainer">
  <!-- language-python is a prism.js class -->
  <table class="language-python">
    {#each sortedData as [k, v]}
      <ArtifactRow id={k} artifact={v} />
    {/each}
  </table>
</div>

<style>
  .tableContainer {
    width: 100%;
    overflow: auto;
  }

  table {
    width: 100%;
  }
</style>
