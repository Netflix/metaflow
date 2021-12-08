<!-- Draws a DAG (Directed Acyclic Graph) based on the input steps -->
<script context="module">
  export const currentStepContext = "currentStep";
</script>

<script lang="ts">
  import "./dag.css";
  import Connectors from "./connectors.svelte";
  import StepWrapper from "./step-wrapper.svelte";
  import { onMount, setContext } from "svelte";
  import type { Boxes, DagComponent } from "../../types";
  import { cardData } from "../../store";
  import { getStepNameFromPathSpec } from "../../utils";

  export let componentData: DagComponent;

  const { data: steps } = componentData;
  let boxes: Boxes = {};
  let el: HTMLElement;
  let box: DOMRect;
  let innerWidth: number;
  let boxTop: number;
  let boxLeft: number;

  setContext(
    currentStepContext,
    getStepNameFromPathSpec($cardData?.metadata?.pathspec)
  );

  let resizeTimeout: NodeJS.Timeout;
  const RESIZE_TIMEOUT = 300;
  let resizing: boolean = false;

  const getContainerPosition = () => {
    box = el?.getBoundingClientRect();
  };

  // Debounce the resizing so the processor doesn't get overloaded
  const handleResize = (): void => {
    resizing = true;
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
      getContainerPosition();
      resizing = false;
    }, RESIZE_TIMEOUT);
  };

  onMount(getContainerPosition);

  $: boxTop = box?.top ?? 0;
  $: boxLeft = box?.left ?? 0;
</script>

<svelte:window bind:innerWidth on:resize={handleResize} />

<div bind:this={el} style="position: relative">
  {#if steps?.start}
    <StepWrapper {steps} stepName="start" bind:boxes {innerWidth} />
  {:else}
    <p>No start step</p>
  {/if}
  {#if !resizing && Object.keys(boxes).length}
    <Connectors {boxes} {steps} top={boxTop} left={boxLeft} />
  {/if}
</div>
