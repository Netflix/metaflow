<!-- This component gives us a reusable modal, 
  it takes any card component as an input and will attempt to render -->
<script lang="ts">
  import type * as types from "../types";
  import { modal } from "../store";
  import Icon from "@iconify/svelte";
  import ComponentRenderer from "./card-component-renderer.svelte";
  export let componentData: types.CardComponent;

  function handleEscapeKey(e: KeyboardEvent): void {
    if (e.code === "Escape") {
      modal.set(undefined);
    }
  }

  function handleModalClick(e: MouseEvent): void {
    e.stopImmediatePropagation();
    modal.set(undefined);
  }
</script>

{#if componentData && $modal}
  <div class="modal" on:click={handleModalClick}>
    <span class="cancelButton">
      <Icon icon="mdi:close" />
    </span>
    <div
      class="modalContainer"
      on:click={(e) => {
        e?.stopImmediatePropagation();
      }}
    >
      <ComponentRenderer componentData={$modal} />
    </div>
  </div>
{/if}

<svelte:window on:keyup={handleEscapeKey} />

<style>
  .modal {
    background: rgba(0, 0, 0, 0.5);
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    z-index: 100;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    height: 100%;
    overflow: hidden;
    cursor: pointer;
  }

  :global(.modalContainer > *) {
    position: relative;
    background-color: white;
    border-radius: 5px;
    flex: 0 1 auto;
    cursor: default;
    padding: 1rem;
  }

  :global(.modal img) {
    max-height: 80vh !important;
  }

  .cancelButton {
    position: absolute;
    top: 1rem;
    right: 1rem;
    color: white;
    font-size: 2rem;
    cursor: pointer;
  }

  .cancelButton:hover {
    color: var(--blue);
  }
</style>
