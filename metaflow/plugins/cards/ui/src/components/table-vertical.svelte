<!-- There are cases where we want a vertical table.  Its expected to be a very low column 
  count.  We basically pivot the table and style the first column to be headers -->
<script lang="ts">
  import type * as types from "../types";
  import DataRenderer from "./table-data-renderer.svelte";

  export let componentData: types.TableComponent;
  $: ({ columns, data } = componentData);
</script>

{#if columns && data}
  <div class="tableContainer" data-component="table-vertical">
    <table>
      <tbody>
        <!-- here we're pivoting the table treating columns as rows -->
        {#each columns as column, i}
          <tr>
            <td class="labelColumn">{column}</td>
            {#each data as row}
              <td><DataRenderer componentData={row[i]} /></td>
            {/each}
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<style>
  td {
    text-align: left;
  }

  td.labelColumn {
    text-align: right;
    background-color: var(--lt-grey);
    font-weight: 700;
    /* note, if you are going to change the default width, please do the same in artifacts */
    width: 12%;
    white-space: nowrap;
  }
</style>
