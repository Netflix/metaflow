<!-- A wrapper for each Step and its children. Handles joins and foreaches -->
<script lang="ts">
  import { onMount } from "svelte";
  import type { Boxes, Dag } from "../../types";
  import Step from "./step.svelte";

  export let steps: Dag;
  export let stepName: string;
  export let levels = 0;
  export let boxes: Boxes = {};

  let stepElement: HTMLElement | null = null;

  const setBox = () => {
    if (stepElement) {
      boxes[stepName] = stepElement;
    }
  };

  onMount(setBox);

  let currentStep = steps[stepName];
  if (!currentStep) {
    console.warn("step ", stepName, " not found");
  }

  // For a static analysis, increase the level for a foreach and decrease it for a join
  const childLevels =
    currentStep?.type === "foreach"
      ? levels + 1
      : currentStep?.type === "join"
        ? levels - 1
        : levels;

  let hasNext = currentStep?.next?.find((nextStepName) => {
    return steps[nextStepName]?.type !== "join";
  });
</script>

{#if currentStep}
  <div class="stepwrapper">
    <Step
      name={stepName}
      numLevels={levels}
      step={currentStep}
      bind:el={stepElement}
    />

    {#if hasNext}
      <div class="gap" />
      <div class="childwrapper">
        {#each currentStep.next as nextStepName}
          <svelte:self
            {steps}
            stepName={nextStepName}
            levels={childLevels}
            {boxes}
          />
        {/each}
      </div>
    {/if}
    {#if currentStep.box_ends}
      <div class="gap" />
      <svelte:self {steps} stepName={currentStep.box_ends} {levels} {boxes} />
    {/if}
  </div>
{/if}

<style>
  .stepwrapper {
    display: flex;
    align-items: center;
    flex-direction: column;
    width: 100%;
    position: relative;
    min-width: var(--dag-step-width);
  }

  .childwrapper {
    display: flex;
    width: 100%;
  }

  .gap {
    height: var(--dag-gap);
  }
</style>
