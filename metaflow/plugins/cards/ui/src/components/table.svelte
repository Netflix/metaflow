<!-- this component exists because we support two types 
 of tables.  And we want to avoid the circular dependency.-->
<script lang="ts">
  import type * as types from "../types";
  import TableVertical from "./table-vertical.svelte";
  import TableHorizontal from "./table-horizontal.svelte";

  export let componentData: types.TableComponent;
  $: ({ columns, data, vertical } = componentData);

  const component = vertical ? TableVertical : TableHorizontal;
</script>

{#if columns && data}
  <svelte:component this={component} {componentData} />
{/if}
