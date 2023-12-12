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
  <!-- svelte-ignore a11y-click-events-have-key-events a11y-no-static-element-interactions -->
  <div class="modal" on:click={handleModalClick} data-component="modal">
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
    align-items: center;
    background: rgba(0, 0, 0, 0.5);
    bottom: 0;
    cursor: pointer;
    display: flex;
    height: 100%;
    justify-content: center;
    left: 0;
    overflow: hidden;
    position: fixed;
    right: 0;
    top: 0;
    width: 100%;
    z-index: 100;
  }

  :global(.modalContainer > *) {
    background-color: white;
    border-radius: 5px;
    cursor: default;
    flex: 0 1 auto;
    padding: 1rem;
    position: relative;
  }

  :global(.modal img) {
    max-height: 80vh !important;
  }

  .cancelButton {
    color: white;
    cursor: pointer;
    font-size: 2rem;
    position: absolute;
    right: 1rem;
    top: 1rem;
  }

  .cancelButton:hover {
    color: var(--blue);
  }
</style>
