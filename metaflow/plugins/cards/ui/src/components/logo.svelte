<!-- This component gives us an option for a reusable logo, it supports light/dark -->
<script lang="ts">
  export let light: boolean = false;
</script>

{#if light}
  <img src="mf_logo_wht.svg" alt="Metaflow Logo" />
{:else}
  <img src="mf_logo_blk.svg" alt="Metaflow Logo" />
{/if}

<style>
  img {
    display: inline-block;
  }
</style>
