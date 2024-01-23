<!-- This component is used for a simple subtitle text -->
<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.SubtitleComponent;
</script>

<p class="subtitle" data-component="subtitle">{componentData?.text || ""}</p>

<style>
  .subtitle {
    font-size: 1rem;
    text-align: left;
  }
</style>
