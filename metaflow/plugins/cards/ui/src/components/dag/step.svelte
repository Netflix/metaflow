<!-- Renders a single step in a DAG. -->
<script lang="ts">
  import type { DagStep } from "../../types";
  import { getContext, onMount } from "svelte";
  import { isOverflown } from "../../utils";
  import { currentStepContext } from "./constants.svelte";

  export let name: string;
  export let step: DagStep;
  export let numLevels = 0;
  export let el: HTMLElement | null;

  let success = false;
  let error = false;
  let running = false;

  // Calculate number of levels in the stack under the step rectangle
  let levelsToShow: string;
  const offset = 4;
  const numPerLevel = 3;

  // Show the number of levels on the right if the number of tasks > the number on the left
  const levelCutoffs: Record<number, number> = {
    3: 3,
    100: 4,
    1000: 5,
    10000: 6,
  };

  let levels: string[] = [];

  if (step.num_possible_tasks) {
    if (step.num_possible_tasks > 1) {
      levelsToShow = new Intl.NumberFormat().format(step.num_possible_tasks);
    }
    numLevels = step.num_possible_tasks - 1;

    Object.keys(levelCutoffs).forEach((cutoff) => {
      const cutoffNumber = Number.parseInt(cutoff);
      if (step.num_possible_tasks && step.num_possible_tasks > cutoffNumber) {
        numLevels = levelCutoffs[cutoffNumber];
      }
    });
  } else {
    numLevels *= numPerLevel;
  }

  if (numLevels > 0) {
    levels = new Array<string>(numLevels).fill("");
  }

  // For each of the levels in the stack show a status with errors first, then successes
  levels = levels.map((_, i) => {
    if (step.num_possible_tasks) {
      const numFailed = step.num_failed ?? 0;
      const numSuccessful = step.successful_tasks ?? 0;

      if (
        (numFailed - 1) / step.num_possible_tasks >=
        (i + 1) / levels.length
      ) {
        return "error";
      }
      if (
        (numFailed + numSuccessful) / step.num_possible_tasks >=
        (i + 1) / levels.length
      ) {
        return "success";
      } else {
        return "running";
      }
    }
    return "";
  });

  const currentStep: string = getContext(currentStepContext);
  const current = name === currentStep;

  // Set color of the step based on the results
  if (step.failed || step.num_failed) {
    error = true;
  } else {
    if ((step.num_possible_tasks ?? 0) > (step.successful_tasks ?? 0)) {
      running = true;
    } else {
      if (
        step.num_possible_tasks &&
        step.num_possible_tasks === step.successful_tasks
      ) {
        success = true;
      }
    }
  }
  let descriptionEl: HTMLElement;
  let overflown = false;

  onMount(() => {
    overflown = isOverflown(descriptionEl);
  });
</script>

<div class="wrapper" class:current>
  {#if levelsToShow}
    <div class="levelstoshow">x{levelsToShow}</div>
  {/if}
  <div
    class="step rectangle"
    class:success
    class:running
    class:error
    bind:this={el}
  >
    <div class="inner">
      <span class="name">{name}</span>
      <div
        class="description"
        class:overflown
        title={overflown ? step.doc : undefined}
        bind:this={descriptionEl}
      >
        {step.doc}
      </div>
    </div>
  </div>
  {#each levels as statusClass, i}
    <div
      class="level rectangle {statusClass}"
      style="z-index: {(i + 1) * -1}; top: {(i + 1) * offset}px; left: {(i +
        1) *
        offset}px;"
    />
  {/each}
</div>

<style>
  .wrapper {
    position: relative;
    z-index: 1;
  }

  .step {
    font-size: 0.75rem;
    padding: 0.5rem;
    color: var(--dk-grey);
  }

  .rectangle {
    background-color: var(--dag-bg-static);
    border: 1px solid var(--dag-border);
    box-sizing: border-box;
    position: relative;
    height: var(--dag-step-height);
    width: var(--dag-step-width);
  }

  .rectangle.error {
    background-color: var(--dag-bg-error);
  }

  .rectangle.success {
    background-color: var(--dag-bg-success);
  }

  .rectangle.running {
    background-color: var(--dag-bg-running);
  }

  .level {
    z-index: -1;
    filter: contrast(0.5);
    position: absolute;
  }

  .inner {
    position: relative;
    height: 100%;
    width: 100%;
  }

  .name {
    font-weight: bold;
    overflow: hidden;
    text-overflow: ellipsis;
    display: block;
  }

  .description {
    position: absolute;
    max-height: 4rem;
    bottom: 0;
    left: 0;
    right: 0;
    overflow: hidden;
    -webkit-line-clamp: 4;
    line-clamp: 4;
    display: -webkit-box;
    -webkit-box-orient: vertical;
  }

  .overflown.description {
    cursor: help;
  }

  .current .rectangle {
    box-shadow: 0 0 10px var(--dag-selected);
  }

  .levelstoshow {
    position: absolute;
    bottom: 100%;
    right: 0;
    font-size: 0.75rem;
    font-weight: 100;
    text-align: right;
  }
</style>
