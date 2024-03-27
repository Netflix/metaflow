<!-- this creates a section wrapper to hold multiple components inside it -->
<script lang="ts">
  import type * as types from "../types";
  import Title from "./title.svelte";
  import Subtitle from "./subtitle.svelte";

  export let componentData: types.HeadingComponent;
  $: ({ title, subtitle } = componentData);
</script>

<header class="container" data-component="heading">
  {#if title}
    <Title componentData={{ type: "title", text: title }} />
  {/if}
  {#if subtitle}
    <Subtitle componentData={{ type: "subtitle", text: subtitle }} />
  {/if}
</header>

<style>
  header {
    margin-bottom: var(--component-spacer);
  }
</style>
