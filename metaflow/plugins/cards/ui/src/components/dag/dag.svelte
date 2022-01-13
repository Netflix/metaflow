<!-- Draws a DAG (Directed Acyclic Graph) based on the input steps -->
<script lang="ts">
  import "./dag.css";
  import Connectors from "./connectors.svelte";
  import StepWrapper from "./step-wrapper.svelte";
  import { setContext } from "svelte";
  import type { Boxes, DagComponent } from "../../types";
  import { cardData } from "../../store";
  import { getFromPathSpec } from "../../utils";
  import { currentStepContext } from "./constants.svelte";

  export let componentData: DagComponent;

  const { data: steps } = componentData;
  let boxes: Boxes = {};
  let el: HTMLElement;

  setContext(
    currentStepContext,
    getFromPathSpec($cardData?.metadata?.pathspec, "stepname")
  );

  let resizeTimeout: ReturnType<typeof setTimeout>;
  const RESIZE_TIMEOUT = 100;
  let resizing = false;

  // Debounce the resizing so the processor doesn't get overloaded
  const handleResize = (): void => {
    resizing = true;
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
      resizing = false;
    }, RESIZE_TIMEOUT);
  };
</script>

<svelte:window on:resize={handleResize} />

<div
  bind:this={el}
  style="position: relative; line-height: 1"
  data-component="dag"
>
  {#if steps?.start}
    <StepWrapper {steps} stepName="start" bind:boxes />
  {:else}
    <p>No start step</p>
  {/if}
  {#if !resizing && Object.keys(boxes).length}
    <Connectors {boxes} {steps} container={el} />
  {/if}
</div>
