<!-- This component will render a simple log with syntax highlighting -->
<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.LogComponent;
  let el: HTMLElement;

  function highlightCode() {
    el && (window as any)?.Prism?.highlightElement(el);
  }

  $: el ? highlightCode() : null;
</script>

<pre class="log" data-component="log"> 
  <code class="mono language-log" bind:this={el}>
    {componentData.data}
  </code>
</pre>

<style>
  .log {
    background: var(--lt-grey) !important;
    font-size: 0.9rem;
    padding: 2rem;
  }
</style>
