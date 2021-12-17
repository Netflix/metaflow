<!-- This component assembles the data rows of the artifacts table -->
<script lang="ts">
  import type * as types from "../types";
  export let id: string;
  export let artifact: types.Artifact;

  let el: HTMLElement;

  function highlightCode() {
    // after prism highlights it adds the class, so we're making sure it only loads once
    if (el && !el.classList.contains("language-python")) {
      if (typeof window !== "undefined") {
        (window as any)?.Prism?.highlightElement(el);
      }
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
  .idCell {
    font-weight: bold;
    text-align: right;
    background: var(--lt-grey);
    /* note, if you are going to change the default width, please do the same in vertical-table */
    width: 12%;
  }

  .codeCell {
    text-align: left;
    user-select: all;
  }
</style>
