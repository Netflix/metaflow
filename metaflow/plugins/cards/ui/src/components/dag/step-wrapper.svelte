<!-- A wrapper for each Step and its children. Handles joins and foreaches -->
<script lang="ts">
  import { onMount } from "svelte";
  import type { Dag, DagStructure } from "../../types";
  import Step from "./step.svelte";

  export let steps: Dag;
  export let stepName: string;
  export let levels = 0;
  export let joins: string[] = []
  export let pathToStep: string = "";
  export let dagStructure: DagStructure = {};

  let stepElement: HTMLElement | null = null;


  const fullStepPath = pathToStep ? `${pathToStep}/${stepName}` : stepName;
  const currentStep = steps[stepName];

  // Register node to dag structure with HTML node and correct connections
  const registerNode = () => {
    if (dagStructure[fullStepPath]) {
      console.log("Node already registered:", fullStepPath);
      return;
    }
    if (!stepElement) {
      console.warn("Step element not found:", fullStepPath);
      return;
    }

    const connections = []
    for (const nextStep of currentStep.next) {
      const nextStepObj = steps[nextStep]
      // If next step is a join, find the corresponding join connection from props
      // joins are rendered in box_ends section so they will not be rendered as direct
      // children for this step
      if (nextStepObj?.type === "join") {
        const fromJoins = joins.find(str => str.endsWith("/" + nextStep))
        if (fromJoins) {
          connections.push(fromJoins)
        }
      } else {
        if (nextStep === 'end') {
          connections.push("end");
        } else {
          if (nextStep === stepName) {
            connections.push(fullStepPath)
          } else {
            connections.push(fullStepPath + "/" + nextStep)
          }
        }
      }
    }

    dagStructure[fullStepPath] = {
      stepName,
      pathToStep,
      connections,
      node: stepElement
    };
  };

  onMount(registerNode);

  let hasNext = currentStep?.next?.find((nextStepName) => {
    return steps[nextStepName]?.type !== "join" && nextStepName !== 'end';
  });

  // For a static analysis, increase the level for a foreach and decrease it for a join
  const childLevels =
    currentStep?.type === "foreach"
      ? levels + 1
      : currentStep?.type === "join"
        ? levels - 1
        : levels;

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
          {#if nextStepName === stepName}
            <!-- noop -->
          {:else if steps[nextStepName].type !== 'join' && nextStepName !== 'end'}
            <svelte:self
              {steps}
              stepName={nextStepName}
              levels={childLevels}
              {dagStructure}
              pathToStep={fullStepPath}
              joins={currentStep.box_ends ? [fullStepPath + "/" + currentStep.box_ends, ...joins] : joins}
            />
            {:else}
            <div class="stepwrapper"></div>
          {/if}
        {/each}
      </div>
    {/if}
    {#if currentStep.box_ends}
      <div class="gap" />
      <svelte:self {steps} stepName={currentStep.box_ends} {levels} {dagStructure} pathToStep={fullStepPath} joins={joins} />
    {/if}
    {#if stepName === 'start'}
      <div class="gap" />
      <svelte:self {steps} stepName="end" {levels} {dagStructure} />
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
