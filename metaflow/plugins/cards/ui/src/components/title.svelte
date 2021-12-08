<!-- This component renders a title for use throughout your page -->
<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.TextComponent;
  const { text } = componentData;
</script>

<h2 class="title">
  {text}
</h2>

<style>
  .title {
    font-size: 1.5rem;
    font-weight: 600;
    text-align: left;
  }
</style>
