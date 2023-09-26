<!-- A simple text component to render a paragraph wherever needed.App
  If you need more than one paragraph, you should probably use the HTML component instead -->
<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.ProgressBarComponent;
  $: ({ max, id, value, label, unit } = componentData);
</script>

<div>
  {#if label}
    <label for={id}>{label} ({value}{unit || ""})</label>
  {/if}
  <progress {id} {max} {value}>{value}{unit || ""}</progress>
</div>
