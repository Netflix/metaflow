<!-- This component will render a simple log with syntax highlighting -->
<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.PythonCodeComponent;
  let el: HTMLElement;

  function highlightCode() {
    el && (window as any)?.Prism?.highlightElement(el, );
  }

  $: el ? highlightCode() : null;
</script>

<!-- This needs to be in this exact format of <pre><code> ... without a new line between the <pre> and <code> tags -->
<!-- Need to do this to avoid weird indentation issues -->
<!-- based on https://github.com/PrismJS/prism/issues/554#issuecomment-83197995 -->
<pre data-component="pythonCode"><code class="language-python" bind:this={el}>{componentData.data}
  </code>
</pre>
