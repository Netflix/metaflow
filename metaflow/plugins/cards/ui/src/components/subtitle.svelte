<!-- This component is used for a simple subtitle text -->
<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.SubtitleComponent;
</script>

<p class="subtitle">{componentData.text}</p>

<style>
  .subtitle {
    font-size: 1rem;
    line-height: 1.66;
    font-weight: 400;
    text-align: left;
    margin-bottom: 3.5rem;
  }
</style>
