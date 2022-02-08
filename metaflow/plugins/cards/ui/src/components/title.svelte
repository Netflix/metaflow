<!-- This component renders a title for use throughout your page -->
<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.TitleComponent;
  const { text } = componentData;
</script>

<h2 class="title" data-component="title">
  {text}
</h2>

<style>
  .title {
    text-align: left;
  }
</style>
