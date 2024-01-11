<!-- Draws all the connectors between Steps in a DAG -->
<script lang="ts">
  import type { Boxes, Dag } from "../../types";
  import { convertPixelsToRem } from "../../utils";
  import Connector from "./connector.svelte";

  export let steps: Dag;
  export let boxes: Boxes;
  export let container: HTMLElement;

  interface ConnectorData {
    top: number;
    left: number;
    bottom: number;
    right: number;
  }

  let connectors: ConnectorData[] = [];

  $: {
    connectors = [];
    const containerBox = container.getBoundingClientRect();
    const top = containerBox.top;
    const left = containerBox.left;

    boxes &&
      Object.keys(steps).forEach((stepName) => {
        const currentStep = steps[stepName];
        const currentBox = boxes[stepName].getBoundingClientRect();

        // for each next step, calculate the position of the connector from this step
        currentStep.next?.forEach((nextStep) => {
          const nextBox = boxes[nextStep].getBoundingClientRect();
          const newConnectorData: ConnectorData = {
            top: currentBox.bottom - top,
            left: currentBox.left - left + currentBox.width / 2,
            bottom: nextBox.top - top,
            right: nextBox.left - left + nextBox.width / 2,
          };
          connectors = [...connectors, newConnectorData];
        });
      });
  }
</script>

{#each connectors as connector}
  <Connector
    top={convertPixelsToRem(connector.top)}
    left={convertPixelsToRem(connector.left)}
    bottom={convertPixelsToRem(connector.bottom)}
    right={convertPixelsToRem(connector.right)}
  />
{/each}
