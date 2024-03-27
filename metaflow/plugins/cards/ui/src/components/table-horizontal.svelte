<!-- This is the standard table component.  It will attempt to make fixed headers, and 
      allow you to scroll -->
<script lang="ts">
  import type * as types from "../types";
  import DataRenderer from "./table-data-renderer.svelte";

  export let componentData: types.TableComponent;
  $: ({ columns, data } = componentData);
</script>

{#if columns && data}
  <div class="tableContainer" data-component="table-horizontal">
    <table>
      <thead>
        <tr>
          {#each columns as column}
            <th>{column}</th>
          {/each}
        </tr>
      </thead>
      <tbody>
        {#each data as row}
          <tr>
            {#each row as col}
              <td><DataRenderer componentData={col} /></td>
            {/each}
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<style>
  .tableContainer {
    overflow: auto;
  }

  th {
    position: sticky;
    top: -1px;
    z-index: 2;
    white-space: nowrap;
    background: var(--white);
  }
</style>
