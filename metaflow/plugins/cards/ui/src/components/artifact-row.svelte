<!-- This component assembles the data rows of the artifacts table -->
<script lang="ts">
  import type * as types from "../types";
  export let id: string;
  export let artifact: types.Artifact;

  let el: HTMLElement;

  function highlightCode() {
    // after prism highlights it adds the class, so we're making sure it only loads once
    if (el && !el.classList.contains("language-python")) {
      (window as any)?.Prism?.highlightElement(el);
    }
  }

  $: el ? highlightCode() : null;
</script>

<tr>
  <td class="idCell"> {id} </td>
  <td class="codeCell"
    ><code class="mono" bind:this={el}>{artifact.data}</code></td
  >
</tr>

<style>
  td {
    background: var(--lt-grey);
    border: none;
    text-align: left;
  }

  tr {
    border-bottom: 1px solid var(--grey);
  }

  .idCell {
    font-weight: bold;
    border-right: 1px dashed grey;
    text-align: right;
  }

  .codeCell {
    text-align: left;
    background: white;
    user-select: all;
  }
</style>
